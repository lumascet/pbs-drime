"""In-memory mock of the Drime API subset used by drime-s3gw (for tests only).

Behaves like the real API as far as it is documented / observed in rclone:
- duplicate names in a folder get " (1)" appended (exercises the gateway's rename),
- downloads redirect to a different host, which must NOT receive the Bearer token,
- delete moves to trash first; deleteForever removes; unknown ids -> 422 "invalid",
- optional fault injection (fail_rate): 429s, 500s before processing and, for
  uploads, 500s *after* the file was stored (ambiguous failures).
"""
from __future__ import annotations

import hashlib
import http.server
import json
import random
import re
import threading
import time
import urllib.parse
import uuid

TOKEN = "testtoken"


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.next_id = 1
        self.entries: dict[int, dict] = {}
        self.uploads: dict[str, dict] = {}
        self.fail_rate = 0.0
        self.latency = 0.0  # seconds added to every API call (not to storage PUT/GET)
        self.calls: dict[str, int] = {}
        self.auth_leaks = 0
        self.alt_host = ""  # host:port used for download redirects

    def new_id(self):
        i = self.next_id
        self.next_id += 1
        return i

    def children(self, parent):
        return [e for e in self.entries.values() if e["parent_id"] == parent and not e["deleted"]]

    def unique_name(self, parent, name):
        names = {e["name"] for e in self.children(parent)}
        if name not in names:
            return name
        n = 1
        while f"{name} ({n})" in names:
            n += 1
        return f"{name} ({n})"

    def add(self, parent, name, typ, data=b""):
        i = self.new_id()
        h = uuid.uuid4().hex[:14]
        e = {"id": i, "name": self.unique_name(parent, name), "type": typ, "parent_id": parent,
             "file_size": len(data), "hash": h, "url": f"api/v1/file-entries/download/{h}" if typ != "folder" else None,
             "data": data, "deleted": False, "created_at": "2026-11-04T10:00:00.000000Z",
             "updated_at": "2026-11-04T10:00:00.000000Z"}
        self.entries[i] = e
        return e


def public(e):
    return {k: v for k, v in e.items() if k not in ("data", "deleted")}


def make_handler(state: State):
    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _json(self, status, obj, headers=None):
            body = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _raw(self, status, body=b"", headers=None):
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(n) if n else b""

        def do_GET(self):
            self._handle("GET")

        def do_POST(self):
            self._handle("POST")

        def do_PUT(self):
            self._handle("PUT")

        def _handle(self, method):
            body = self._body()
            u = urllib.parse.urlsplit(self.path)
            q = dict(urllib.parse.parse_qsl(u.query, keep_blank_values=True))
            path = u.path
            ep = re.sub(r"/\d+$", "/{id}", re.sub(r"/[0-9a-f]{14}$", "/{hash}", path))
            with state.lock:
                state.calls[f"{method} {ep}"] = state.calls.get(f"{method} {ep}", 0) + 1
            # storage endpoints (presigned / redirect target): no bearer auth expected
            if path.startswith("/presigned/") or path.startswith("/r2/"):
                if self.headers.get("Authorization"):
                    state.auth_leaks += 1
                    return self._raw(400, b"<Error>InvalidArgument: multiple auth</Error>")
                return self._storage(method, path, body)
            if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                return self._json(401, {"message": "Unauthenticated."})
            if state.latency:
                time.sleep(state.latency)
            r = random.random()
            if r < state.fail_rate / 3:
                return self._json(429, {"message": "Too Many Attempts."}, {"Retry-After": "0"})
            if r < state.fail_rate * 2 / 3:
                return self._json(500, {"message": "Server Error"})
            ambiguous = r < state.fail_rate
            with state.lock:
                return self._api(method, path, q, body, ambiguous)

        def _storage(self, method, path, body):
            parts = path.split("/")
            if parts[1] == "presigned" and method == "PUT":
                up = state.uploads.get(parts[2])
                if up is None:
                    return self._raw(404)
                up["parts"][int(parts[3])] = body
                return self._raw(200, headers={"ETag": f'"{hashlib.md5(body).hexdigest()}"'})
            if parts[1] == "r2" and method == "GET":
                e = state.entries.get(int(parts[2]))
                if e is None or e["deleted"]:
                    return self._raw(404)
                return self._raw(200, e["data"], {"Content-Type": "application/octet-stream"})
            return self._raw(405)

        def _api(self, method, path, q, body, ambiguous):
            p = path[len("/api/v1"):]
            if method == "GET" and p == "/users/me":
                return self._json(200, {"user": {"id": 1, "email": "test@example.invalid"}})
            if method == "GET" and p == "/user/space-usage":
                used = sum(e["file_size"] for e in state.entries.values())
                return self._json(200, {"used": used, "available": 10 ** 12})
            if method == "GET" and p == "/drive/file-entries":
                parent = int(q["folderId"]) if q.get("folderId") else None
                items = sorted(state.children(parent), key=lambda e: e["id"])
                if q.get("type") == "folder":
                    items = [e for e in items if e["type"] == "folder"]
                per, page = int(q.get("perPage", 50)), int(q.get("page", 1))
                last = max(1, -(-len(items) // per))
                data = [public(e) for e in items[(page - 1) * per: page * per]]
                return self._json(200, {"current_page": page, "last_page": last, "per_page": per,
                                        "total": len(items), "data": data})
            if method == "POST" and p == "/folders":
                j = json.loads(body)
                e = state.add(j.get("parentId"), j["name"], "folder")
                return self._json(200, {"status": "success", "folder": public(e)})
            if method == "POST" and p == "/uploads":
                fields, fdata, fname = parse_multipart(self.headers["Content-Type"], body)
                parent = int(fields["parentId"]) if fields.get("parentId") else None
                if parent is not None and (parent not in state.entries or state.entries[parent]["deleted"]):
                    return self._json(422, {"message": "The selected parent id is invalid."})
                e = state.add(parent, fname, "file", fdata)
                if ambiguous:
                    return self._json(500, {"message": "Server Error"})
                return self._json(201, {"status": "success", "fileEntry": public(e)})
            if method == "POST" and p == "/s3/multipart/create":
                j = json.loads(body)
                uid, key = uuid.uuid4().hex, f"uploads/{uuid.uuid4()}/{uuid.uuid4()}"
                state.uploads[uid] = {"key": key, "size": j["size"], "parts": {}, "done": False}
                return self._json(200, {"key": key, "uploadId": uid, "acl": "private", "status": "success"})
            if method == "POST" and p == "/s3/multipart/batch-sign-part-urls":
                j = json.loads(body)
                host = self.headers["Host"]
                urls = [{"partNumber": n, "url": f"http://{host}/presigned/{j['uploadId']}/{n}?X-Amz-Signature=x"}
                        for n in j["partNumbers"]]
                return self._json(200, {"urls": urls, "status": "success"})
            if method == "POST" and p == "/s3/multipart/complete":
                j = json.loads(body)
                up = state.uploads.get(j["uploadId"])
                if up is None:
                    return self._json(404, {"message": "upload not found"})
                for part in j["parts"]:
                    got = up["parts"].get(part["PartNumber"])
                    if got is None or f'"{hashlib.md5(got).hexdigest()}"' != part["ETag"]:
                        return self._json(400, {"message": "bad part"})
                up["data"] = b"".join(up["parts"][n] for n in sorted(up["parts"]))
                up["done"] = True
                return self._json(200, {"location": "x", "status": "success"})
            if method == "POST" and p == "/s3/multipart/abort":
                state.uploads.pop(json.loads(body)["uploadId"], None)
                return self._json(200, {"status": "success"})
            if method == "POST" and p == "/s3/entries":
                j = json.loads(body)
                up = next((u for u in state.uploads.values() if u["key"].endswith("/" + j["filename"])), None)
                if up is None or not up["done"]:
                    return self._json(422, {"message": "upload not found"})
                if len(up["data"]) != j["size"]:
                    return self._json(422, {"message": "size mismatch"})
                e = state.add(j.get("parentId"), j["clientName"], "file", up["data"])
                if ambiguous:
                    return self._json(500, {"message": "Server Error"})
                return self._json(200, {"fileEntry": public(e)})
            m = re.fullmatch(r"/file-entries/download/([0-9a-f]+)", p)
            if method == "GET" and m:
                e = next((e for e in state.entries.values() if e["hash"] == m.group(1) and not e["deleted"]), None)
                if e is None:
                    return self._json(404, {"message": "not found"})
                return self._raw(302, headers={"Location": f"http://{state.alt_host}/r2/{e['id']}?sig=1"})
            if method == "POST" and p == "/file-entries/delete":
                j = json.loads(body)
                ids = j["entryIds"]
                if any(i not in state.entries for i in ids):
                    return self._json(422, {"message": "The entry ids is invalid."})
                for i in ids:
                    self._delete(i, j.get("deleteForever"))
                return self._json(200, {"status": "success"})
            m = re.fullmatch(r"/file-entries/(\d+)", p)
            if method == "POST" and m and self.headers.get("X-HTTP-Method-Override") == "PUT":
                e = state.entries.get(int(m.group(1)))
                if e is None:
                    return self._json(404, {"message": "not found"})
                e["name"] = json.loads(body)["name"]
                return self._json(200, {"status": "success", "fileEntry": public(e)})
            return self._json(404, {"message": f"no route {method} {p}"})

        def _delete(self, i, forever):
            e = state.entries.get(i)
            if e is None:
                return
            if e["type"] == "folder":
                for c in [c for c in state.entries.values() if c["parent_id"] == i]:
                    self._delete(c["id"], forever)
            if forever:
                state.entries.pop(i, None)
            else:
                e["deleted"] = True

    return H


def parse_multipart(ctype, body):
    boundary = re.search(r'boundary="?([^";]+)"?', ctype).group(1).encode()
    fields, fdata, fname = {}, None, None
    for chunk in body.split(b"--" + boundary):
        if not chunk.strip() or chunk.strip() == b"--":
            continue
        head, _, data = chunk.lstrip(b"\r\n").partition(b"\r\n\r\n")
        data = data[:-2] if data.endswith(b"\r\n") else data
        name = re.search(rb'name="([^"]*)"', head).group(1).decode()
        fn = re.search(rb'filename="([^"]*)"', head)
        if fn:
            fdata, fname = data, fn.group(1).decode()
        else:
            fields[name] = data.decode()
    return fields, fdata, fname


def start(port=0):
    state = State()
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), make_handler(state))
    srv.daemon_threads = True
    state.alt_host = f"localhost:{srv.server_address[1]}"
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, state
