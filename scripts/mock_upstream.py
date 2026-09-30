#!/usr/bin/env python3
"""Mock OpenAI-compatible upstream for rust-one-api smoke tests."""
import json, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _read(self):
        n = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        if self.path == "/models":
            auth = self.headers.get("Authorization", "")
            if not auth.startswith("Bearer "):
                self.send_response(401); self.end_headers(); return
            body = json.dumps({"object": "list", "data": [{"id": "gpt-4o-mini"}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers(); self.wfile.write(body); return
        import time
        now_ms = int(time.time() * 1000)
        if self.path == "/v1/token_plan/remains":  # minimax
            body = json.dumps({"base_resp": {"status_code": 0, "status_msg": ""}, "model_remains": [{
                "model_name": "general", "current_interval_remaining_percent": 55,
                "current_interval_boost_permille": 0, "current_interval_status": 1,
                "current_interval_start_time": now_ms - 3600_000, "current_interval_end_time": now_ms + 4 * 3600_000,
                "start_time": now_ms - 3600_000, "end_time": now_ms + 4 * 3600_000}]}).encode()
        elif self.path == "/v1/credits":  # charm_hyper
            body = json.dumps({"balance": 10}).encode()  # <= 20 -> warning
        else:
            self.send_response(404); self.end_headers(); return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers(); self.wfile.write(body)

    def do_POST(self):
        if self.path == "/responses":
            return self._responses()
        if self.path != "/chat/completions":
            self.send_response(404); self.end_headers(); return
        req = self._read()
        model = req.get("model", "?")
        if model.endswith("-fail500"):
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error":{"message":"upstream boom"}}'); return
        nousage = model.endswith("-nousage")
        last = ""
        for m in reversed(req.get("messages", [])):
            if m.get("role") == "user":
                c = m.get("content")
                last = c if isinstance(c, str) else "".join(p.get("text", "") for p in c if isinstance(p, dict))
                break
        reply = f"mock-echo[{model}]: {last}"
        if req.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            def chunk(content=None, finish=None, usage=None, tool_calls=None):
                c = {"id": "chatcmpl-mock", "model": model, "choices": [{"index": 0, "delta": {}, "finish_reason": finish}]}
                if content is not None: c["choices"][0]["delta"] = {"content": content}
                if tool_calls is not None: c["choices"][0]["delta"] = {"tool_calls": tool_calls}
                if usage: c["usage"] = usage
                return f"data: {json.dumps(c)}\n\n".encode()
            if "tool" in last:
                # CC-native shape: first delta carries id/type/name, later
                # deltas carry only index + arguments fragment.
                self.wfile.write(chunk(tool_calls=[{"index": 0, "id": "call_mock1", "type": "function",
                    "function": {"name": "get_weather", "arguments": ""}}]))
                self.wfile.write(chunk(tool_calls=[{"index": 0, "function": {"arguments": "{\"ci"}}]))
                self.wfile.write(chunk(tool_calls=[{"index": 0, "function": {"arguments": "ty\":\"SF\"}"}}]))
                self.wfile.write(chunk(finish="tool_calls"))
            else:
                self.wfile.write(chunk(content=reply))
            usage = None if nousage else {"prompt_tokens": 12, "completion_tokens": 7, "total_tokens": 19,
                "prompt_tokens_details": {"cached_tokens": 4}}
            if "tool" in last:
                self.wfile.write(chunk(usage=usage))
            else:
                self.wfile.write(chunk(finish="stop", usage=usage))
            self.wfile.write(b"data: [DONE]\n\n")
            return
        resp = {
            "id": "chatcmpl-mock", "object": "chat.completion", "model": model,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": reply}, "finish_reason": "stop"}],
        }
        if not nousage:
            resp["usage"] = {"prompt_tokens": 12, "completion_tokens": 7, "total_tokens": 19,
                      "prompt_tokens_details": {"cached_tokens": 4}}
        body = json.dumps(resp).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers(); self.wfile.write(body)

    def _responses(self):
        """Mock OpenAI Responses API upstream."""
        req = self._read()
        model = req.get("model", "?")
        last = ""
        inp = req.get("input")
        items = [{"type": "message", "role": "user", "content": inp}] if isinstance(inp, str) else (inp or [])
        for m in reversed(items):
            if m.get("role") == "user" or (m.get("type") == "function_call_output"):
                c = m.get("content", m.get("output", ""))
                last = c if isinstance(c, str) else "".join(
                    p.get("text", "") for p in c if isinstance(p, dict) and p.get("type") in ("input_text", "output_text"))
                break
        reply = f"mock-echo[{model}]: {last}"
        rid = "resp-mock"
        usage = {"input_tokens": 12, "output_tokens": 7, "total_tokens": 19,
                 "input_tokens_details": {"cached_tokens": 4}, "output_tokens_details": {"reasoning_tokens": 0}}
        msg_item = {"id": "msg_0", "type": "message", "role": "assistant", "status": "completed",
                    "content": [{"type": "output_text", "text": reply, "annotations": []}]}
        if req.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            seq = [0]
            def ev(ty, payload):
                payload["type"] = ty; payload["sequence_number"] = seq[0]; seq[0] += 1
                return f"event: {ty}\ndata: {json.dumps(payload)}\n\n".encode()
            skeleton = {"id": rid, "object": "response", "created_at": 1, "status": "in_progress",
                        "model": model, "output": []}
            self.wfile.write(ev("response.created", {"response": skeleton}))
            self.wfile.write(ev("response.output_item.added", {"output_index": 0,
                "item": {**msg_item, "status": "in_progress", "content": []}}))
            self.wfile.write(ev("response.content_part.added", {"item_id": "msg_0", "output_index": 0,
                "content_index": 0, "part": {"type": "output_text", "text": "", "annotations": []}}))
            self.wfile.write(ev("response.output_text.delta", {"item_id": "msg_0", "output_index": 0,
                "content_index": 0, "delta": reply}))
            self.wfile.write(ev("response.output_text.done", {"item_id": "msg_0", "output_index": 0,
                "content_index": 0, "text": reply}))
            self.wfile.write(ev("response.output_item.done", {"output_index": 0, "item": msg_item}))
            full = {**skeleton, "status": "completed", "output": [msg_item], "usage": usage}
            self.wfile.write(ev("response.completed", {"response": full}))
            return
        resp = {"id": rid, "object": "response", "created_at": 1, "status": "completed",
                "model": model, "output": [msg_item], "usage": usage}
        body = json.dumps(resp).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers(); self.wfile.write(body)

HTTPServer(("127.0.0.1", 9100), H).serve_forever()