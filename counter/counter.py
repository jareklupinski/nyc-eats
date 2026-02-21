#!/usr/bin/env python3
"""Unique-IP visitor counter for food.lupin.ski — systemd on 127.0.0.1:8091.

Persists two files:
  .counter      — the current unique-visitor count (plain integer)
  .counter.ips  — newline-delimited set of seen IP addresses

nginx passes the real client IP via X-Real-IP header.
"""

import json
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

COUNTER_FILE = os.environ.get(
    "COUNTER_FILE", "/home/jarek/food.lupin.ski/.counter"
)
IPS_FILE = COUNTER_FILE + ".ips"

_lock = threading.Lock()
_seen_ips: set[str] = set()
_count: int = 0


def _load():
    """Load persisted state on startup."""
    global _count, _seen_ips
    try:
        with open(COUNTER_FILE) as f:
            _count = int(f.read().strip())
    except (FileNotFoundError, ValueError):
        _count = 0
    try:
        with open(IPS_FILE) as f:
            _seen_ips = {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        _seen_ips = set()


def _save():
    """Atomically persist count and IP set."""
    tmp = COUNTER_FILE + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(_count))
    os.replace(tmp, COUNTER_FILE)

    tmp_ips = IPS_FILE + ".tmp"
    with open(tmp_ips, "w") as f:
        f.write("\n".join(sorted(_seen_ips)) + "\n")
    os.replace(tmp_ips, IPS_FILE)


def _hit(ip: str) -> tuple[int, bool]:
    """Record a visit. Returns (count, is_new)."""
    global _count
    with _lock:
        if ip in _seen_ips:
            return _count, False
        _seen_ips.add(ip)
        _count += 1
        _save()
        return _count, True


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api/hit":
            ip = self.headers.get("X-Real-IP", self.client_address[0])
            count, is_new = _hit(ip)
            body = json.dumps({"count": count, "new": is_new}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/count":
            body = json.dumps({"count": _count}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass  # silence request logs (nginx already logs)


if __name__ == "__main__":
    _load()
    print(f"Loaded {_count} unique visitors ({len(_seen_ips)} IPs)", flush=True)
    addr = ("127.0.0.1", int(os.environ.get("COUNTER_PORT", "8091")))
    print(f"Counter listening on {addr[0]}:{addr[1]}", flush=True)
    HTTPServer(addr, Handler).serve_forever()
