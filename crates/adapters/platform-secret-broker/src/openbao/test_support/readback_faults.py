"""Synthetic mTLS fault server: only temporary canaries, never a physical provider."""
import http.server
import json
from pathlib import Path
import ssl
import sys
import threading

directory = Path(sys.argv[1])
values = {}
counts = {"create": 0}
lock = threading.Lock()


def record():
    temporary = directory / "counts.next"
    temporary.write_text(json.dumps(counts))
    temporary.chmod(0o600)
    temporary.replace(directory / "counts.json")


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def respond(self, code, value=None, raw=None):
        encoded = raw if raw is not None else json.dumps(value).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        try:
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass

    def do_GET(self):
        if self.path == "/v1/sys/health":
            return self.respond(200, {"initialized": True, "sealed": False, "cluster_id": "11eaa45d-f250-4cbd-a735-c8df5143be05"})
        if self.headers.get("X-Vault-Token") != "synthetic-fault-token":
            return self.respond(403, {})
        if self.path == "/v1/sys/mounts/auth/cert":
            return self.respond(200, {"data": {"accessor": "auth_cert_fixture", "type": "cert"}})
        if self.path == "/v1/sys/mounts/secrets":
            return self.respond(200, {"data": {"accessor": "kv_fixture", "type": "kv", "options": {"version": "2"}}})
        mode = (directory / "mode").read_text()
        if self.path.startswith("/v1/secrets/data/") and self.path.endswith("?version=1"):
            if mode == "always-denied":
                return self.respond(403, {})
            path = self.path.removeprefix("/v1/secrets/data/").removesuffix("?version=1")
            if path not in values:
                return self.respond(404, {})
            if mode in ["denied", "unknown-denied"]:
                return self.respond(403, {})
            if mode == "malformed":
                return self.respond(200, raw=b'{"data":')
            if mode in ["absent", "metadata-denied"]:
                return self.respond(404, {})
            return self.respond(200, {"data": {"data": values[path], "metadata": {"version": 1, "created_time": "2026-09-09T00:00:00Z", "deletion_time": "", "destroyed": False}}})
        if self.path.startswith("/v1/secrets/metadata/"):
            path = self.path.removeprefix("/v1/secrets/metadata/")
            if path in values and mode == "metadata-denied":
                return self.respond(403, {})
            return self.respond(404, {})
        return self.respond(403, {})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 < length <= 128 * 1024:
            return self.respond(400, {})
        body = json.loads(self.rfile.read(length))
        if self.path == "/v1/auth/cert/login":
            if body != {"name": "egress"}:
                return self.respond(403, {})
            return self.respond(200, {"auth": {"client_token": "synthetic-fault-token", "lease_duration": 300, "token_policies": ["platform-egress"], "policies": ["platform-egress"]}})
        if self.headers.get("X-Vault-Token") != "synthetic-fault-token" or not self.path.startswith("/v1/secrets/data/"):
            return self.respond(403, {})
        if body.get("options") != {"cas": 0}:
            return self.respond(400, {})
        path = self.path.removeprefix("/v1/secrets/data/")
        with lock:
            counts["create"] += 1
            record()
            if path in values:
                return self.respond(400, {})
            values[path] = body["data"]
        mode = (directory / "mode").read_text()
        if mode == "unknown-denied":
            return self.respond(400, {})
        return self.respond(200, {"data": {"version": 1}})


server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(directory / "server.pem", directory / "server-key.pem")
context.load_verify_locations(directory / "ca.pem")
context.verify_mode = ssl.CERT_REQUIRED
server.socket = context.wrap_socket(server.socket, server_side=True)
record()
temporary = directory / "port.next"
temporary.write_text(str(server.server_port))
temporary.replace(directory / "port")
server.serve_forever()
