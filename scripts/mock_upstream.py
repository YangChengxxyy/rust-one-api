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
            def chunk(content=None, finish=None, usage=None):
                c = {"id": "chatcmpl-mock", "model": model, "choices": [{"index": 0, "delta": {}, "finish_reason": finish}]}
                if content is not None: c["choices"][0]["delta"] = {"content": content}
                if usage: c["usage"] = usage
                return f"data: {json.dumps(c)}\n\n".encode()
            self.wfile.write(chunk(content=reply))
            usage = None if nousage else {"prompt_tokens": 12, "completion_tokens": 7, "total_tokens": 19,
                "prompt_tokens_details": {"cached_tokens": 4}}
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

HTTPServer(("127.0.0.1", 9100), H).serve_forever()
