# rust-one-api

Rust 版 LLM API 中转站（参考 axonhub 的 Go 实现重写）：多协议接入与互通转发、按 token 精确计费、API Key 配额、上游渠道配额探测。

## 架构

```
client ──► inbound transformer ──► unified Request ──► 编排(鉴权/配额/选渠道/重试/计费)
                                                          │
provider ◄── outbound transformer ◄── unified Response ◄──┘
```

- `crates/llm` — 纯协议层（零业务依赖）：统一 `Request/Response/StreamChunk/Usage` 模型、`InboundTransformer`/`OutboundTransformer` trait、SSE 编解码、OpenAI / Anthropic / Gemini 三个 transformer。实例按请求创建（Anthropic 流式需要 content_block 状态机）。
- `src/storage` — sqlx Any（SQLite/PostgreSQL 双后端，URL scheme 决定 migrations 目录），TEXT uuid 主键 + RFC3339 时间戳 + JSON TEXT 列保证可移植。
- `src/pricing` — 价格模型（flat_fee / usage_per_unit / usage_tiered 分段累进 / usage_volume 总量落档），rust_decimal 精确计算，cache/reasoning token 分摊规则见 `compute_cost` 文档。
- `src/orchestrator` — 转发核心：配额预检 → 候选渠道（排除 exhausted）→ 权重排序 + 跨渠道重试（5xx/网络错误换道，4xx 直接透传）→ 模型重映射 → 计费落 usage_log。流式计费在独立 task 中完成，客户端断连不丢账。
- `src/server` — axum 路由：relay 面 + admin 面。
- `src/provider_quota` — 与 axonhub 对齐的配额检测：17 个厂商 checker（OAuth 类 claudecode/codex/github_copilot；API 类 apertis/charm_hyper/cline/commandcode/kimi_code/minimax/nanogpt/neuralwatt/ollama/opencode_go/synthetic/wafer/zenmux/zhipu/zai），统一 `QuotaData` 归一化（limits 去重合并、0.8/1.0 阈值、next_reset_at 汇总），period_cost 从 usage_logs 回填，60s 调度器并发 8 检查，exhausted 渠道被路由排除；无专用 checker 的渠道回落通用探活。

## 快速开始

```bash
cp config.example.yaml config.yaml   # 或直接用环境变量
cargo run
```

冒烟测试（mock 上游）：

```bash
python3 scripts/mock_upstream.py &        # OpenAI 协议 mock，:9100
ROA_ADMIN_TOKEN=admintoken cargo run &    # 网关 :3000

# 建渠道 / 密钥 / 价格
curl -X POST localhost:3000/admin/channels -H 'Authorization: Bearer admintoken' -H 'Content-Type: application/json' -d '{
  "name":"mock","channel_type":"openai/chat_completions","base_url":"http://127.0.0.1:9100",
  "credentials":{"api_key":"x"},"supported_models":["gpt-4o-mini"]}'
curl -X POST localhost:3000/admin/keys -H 'Authorization: Bearer admintoken' -H 'Content-Type: application/json' -d '{"key":"sk-demo"}'
curl -X POST localhost:3000/admin/prices -H 'Authorization: Bearer admintoken' -H 'Content-Type: application/json' -d '{
  "model":"gpt-4o-mini","price":{"items":[
    {"code":"prompt_tokens","pricing":{"mode":"usage_per_unit","unit_price":"0.15","unit_size":1000000}},
    {"code":"completion_tokens","pricing":{"mode":"usage_per_unit","unit_price":"0.60","unit_size":1000000}}]}}'

# 三种协议都能打同一个 OpenAI 上游
curl -X POST localhost:3000/v1/chat/completions -H 'Authorization: Bearer sk-demo' -H 'Content-Type: application/json' -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'
curl -X POST localhost:3000/v1/messages -H 'x-api-key: sk-demo' -H 'Content-Type: application/json' -d '{"model":"gpt-4o-mini","max_tokens":100,"messages":[{"role":"user","content":"hi"}]}'
curl -X POST 'localhost:3000/gemini/v1beta/models/gpt-4o-mini:generateContent?key=sk-demo' -H 'Content-Type: application/json' -d '{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}'
```

## 模型价格目录（参考快照）

启动时（migrate 之后）会从内嵌的 `data/model_prices.seed.yaml` 为 21 个常见模型写入全局（无渠道）价格：OpenAI（gpt-4o/4.1/5 系、o3、o4-mini）、Anthropic（claude-3.5/3.7/4/4.5 系，含 `cache_write_tokens` 5 分钟写入价）、Gemini（2.0/2.5 flash、2.5 pro 阶梯价 ≤200k / >200k tokens 渐进计价）。单位均为美元 / 1M tokens。

该快照为 2026-09 编译时的公开价格汇总，构建时未联网核对厂商页面，生产计费前请以厂商官网为准。价格只是参考初值，可用 `/admin/prices` 随时新增或覆盖（渠道级价格优先于全局价）；仅当 `model_prices` 表为空时才会播种，不会覆盖已有数据。播种失败只记录警告，不阻塞启动。

## API 一览

Relay（`Authorization: Bearer` / `x-api-key` / `?key=`）：

| 路由 | 入站协议 |
|---|---|
| `POST /v1/chat/completions` | OpenAI |
| `POST /v1/messages`、`POST /anthropic/v1/messages` | Anthropic |
| `POST /gemini/{ver}/models/{model}:{generateContent\|streamGenerateContent}` | Gemini |
| `GET /v1/models` | 模型列表（enabled 渠道 supported_models ∪ model_mapping keys，去重排序） |

Admin（`Authorization: Bearer $ROA_ADMIN_TOKEN`）：`/admin/channels`、`/admin/keys`、`/admin/prices`（CRUD），`/admin/usage`（用量查询），`/admin/quota`、`POST /admin/quota/check`（渠道配额）。

计费说明：上游（流式或非流式）未返回 usage 时，网关按启发式估算 token 计费（ASCII ≈ 4 字符/token，CJK（≥ U+2E80）≈ 1 字符/token，每条消息 +3 开销），并打 `tracing::warn`（含 request_id）；usage_logs 无独立标记列。

## 数据约定

- `channel_type` 即出站协议格式：`openai/chat_completions` | `claude/messages` | `gemini/models`。
- `channel.credentials` = `{"api_key": "..."}`；`model_mapping` = `{"请求模型": "上游模型"}`。
- `api_key.quota`（均可选，缺省不限）：`{"max_requests_per_day": N, "max_tokens_per_day": N, "max_cost_per_day": "1.5"}`，按 UTC 自然日聚合 usage_logs 判定。
- 价格按 `(channel_id, model)` 精确匹配，无渠道专属价则回落全局价（`channel_id` 为空）；改价生成新 `reference_id` 供账单追溯。
- Gemini 渠道的 `base_url` 需包含到 models 路径，如 `https://generativelanguage.googleapis.com/v1beta/models`。

## 测试

```bash
cargo test --workspace   # 47 项：llm 协议 29 + storage 5 + pricing 12 + config 1
```
