"""Synthetic HTTPS wire fixture; never used as an installed physical provider."""
import http.server
import json
import os
from pathlib import Path
import ssl
import sys
import threading
import time

directory = Path(sys.argv[1])
counts = {"login": 0, "create": 0, "destroy": 0}
payload = None
destroyed = False
lock = threading.Lock()


def record():
    temporary = directory / "counts.next"
    temporary.write_text(json.dumps(counts))
    temporary.chmod(0o600)
    temporary.replace(directory / "counts.json")


def metadata():
    return {"version": 1, "created_time": "2026-09-09T00:00:00Z", "deletion_time": "", "destroyed": destroyed}


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def respond(self, code, value=None, raw=None):
        encoded = raw if raw is not None else json.dumps(value).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        if code == 307:
            self.send_header("Location", "https://localhost:1/never-follow")
        self.end_headers()
        try:
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass

    def do_GET(self):
        mode = (directory / "mode").read_text().strip()
        if self.path == "/v1/sys/health":
            cluster = "292036e7-f9ab-41fd-8122-077ea91db8b0" if mode == "wrong-cluster" else "092036e7-f9ab-41fd-8122-077ea91db8b0"
            return self.respond(200, {"initialized": True, "sealed": False, "cluster_id": cluster})
        if self.headers.get("X-Vault-Token") != "synthetic-wire-token":
            return self.respond(403, {"errors": ["synthetic denied"]})
        if self.path == "/v1/sys/mounts/auth/insight-cert":
            return self.respond(200, {"data": {"accessor": "auth_cert_abc", "type": "cert"}})
        if self.path == "/v1/sys/mounts/secrets":
            accessor = "kv_replaced" if mode == "wrong-mount" else "kv_abc"
            return self.respond(200, {"data": {"accessor": accessor, "type": "kv", "options": {"version": "2"}}})
        if self.path == "/v1/secrets/data/prepared/canary?version=1":
            if mode == "redirect":
                return self.respond(307, {})
            if mode == "duplicate":
                return self.respond(200, raw=b'{"data":{},"data":{}}')
            if mode == "oversized":
                return self.respond(200, {"extra": "x" * (512 * 1024)})
            if payload is None or destroyed:
                return self.respond(404, {"errors": []})
            return self.respond(200, {"data": {"metadata": metadata(), "data": payload}})
        if self.path == "/v1/secrets/metadata/prepared/canary":
            if payload is None:
                return self.respond(404, {"errors": []})
            return self.respond(200, {"data": {"current_version": 1, "versions": {"1": metadata()}}})
        return self.respond(404, {"errors": []})

    def do_POST(self):
        global payload, destroyed
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/v1/auth/insight-cert/login":
            if body != {"name": "fixture"}:
                return self.respond(403, {"errors": ["synthetic role rejected"]})
            with lock:
                counts["login"] += 1
                record()
            return self.respond(200, {"auth": {"client_token": "synthetic-wire-token", "lease_duration": 300, "token_policies": ["fixture"], "policies": ["fixture"]}})
        if self.headers.get("X-Vault-Token") != "synthetic-wire-token":
            return self.respond(403, {"errors": []})
        if self.path == "/v1/secrets/data/prepared/canary":
            if body.get("options") != {"cas": 0}:
                return self.respond(400, {"errors": []})
            with lock:
                counts["create"] += 1
                record()
                if payload is not None:
                    return self.respond(400, {"errors": ["synthetic generic CAS rejection"]})
                payload = body["data"]
            if (directory / "mode").read_text().strip() == "lost-write-response":
                time.sleep(2)
            return self.respond(200, {"data": {"version": 1}})
        if self.path == "/v1/secrets/destroy/prepared/canary":
            if body != {"versions": [1]}:
                return self.respond(400, {"errors": []})
            with lock:
                counts["destroy"] += 1
                destroyed = True
                record()
            return self.respond(204, raw=b"")
        return self.respond(403, {"errors": []})


server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(directory / "server.pem", directory / "server-key.pem")
context.load_verify_locations(directory / "ca.pem")
context.verify_mode = ssl.CERT_REQUIRED
server.socket = context.wrap_socket(server.socket, server_side=True)
record()
(directory / "port").write_text(str(server.server_port))
server.serve_forever()
