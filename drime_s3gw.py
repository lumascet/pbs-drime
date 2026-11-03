#!/usr/bin/env python3
"""drime-s3gw - a small S3-compatible gateway that stores objects on Drime.

Built so Proxmox Backup Server (4.1.9+) can use Drime as an S3 datastore
backend: backups are written straight to Drime, and verify jobs read every
chunk back from Drime.

Python standard library only (3.11+). See README.md for setup.
"""
from __future__ import annotations

import argparse
import base64
import email.utils
import hashlib
import hmac
import http.client
import http.server
import json
import logging
import os
import random
import re
import socket
import sqlite3
import ssl
import sys
import threading
import time
import tomllib
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from xml.sax.saxutils import escape as _xml_escape

__version__ = "0.1.0"

log = logging.getLogger("drime-s3gw")

MiB = 1024 * 1024
S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"
# Zero-byte objects are stored on Drime as a 1-byte file with this name suffix,
# because empty uploads are not documented to work.
EMPTY_SUFFIX = ".s3gw-empty"
SMALL_UPLOAD_MAX = 5 * MiB  # <= this: POST /uploads; above: S3 multipart flow
PART_SIZE = 5 * MiB  # multipart part size used by Drime's own clients
RETRY_STATUS = {429, 500, 502, 503, 504, 509, 520, 521, 522, 523, 524}
HEX4 = re.compile(r"[0-9a-f]{4}")
HEX2 = re.compile(r"[0-9a-f]{2}")
LIST_PAGE = 1000
TOP = "\U0010ffff"  # sorts after every other code point


# --------------------------------------------------------------------------- config


@dataclass
class Config:
    access_key: str = ""
    secret_key: str = ""
    drime_token: str = ""
    bucket: str = "pbs"
    region: str = "us-west-1"
    listen: str = "127.0.0.1"
    port: int = 8443
    tls: bool = True
    cert: str = "/etc/drime-s3gw/cert.pem"
    key: str = "/etc/drime-s3gw/key.pem"
    drime_api: str = "https://app.drime.cloud/api/v1"
    drime_root: str = "pbs-s3"
    workspace_id: int = 0
    index_db: str = "/var/lib/drime-s3gw/index.sqlite"
    hard_delete: bool = True
    requests_per_minute: int = 1500
    max_inflight: int = 16
    retries: int = 6
    http_timeout: float = 300.0
    max_object_size: int = 256 * MiB
    log_level: str = "INFO"

    @classmethod
    def load(cls, path: str) -> "Config":
        with open(path, "rb") as f:
            raw = tomllib.load(f)
        known = {f.name for f in fields(cls)}
        unknown = set(raw) - known
        if unknown:
            raise SystemExit(f"{path}: unknown option(s): {', '.join(sorted(unknown))}")
        cfg = cls(**raw)
        for name in ("access_key", "secret_key", "drime_token"):
            env = os.environ.get("DRIME_S3GW_" + name.upper())
            if env:
                setattr(cfg, name, env)
        return cfg

    def require(self, *names: str) -> None:
        missing = [n for n in names if not getattr(self, n)]
        if missing:
            raise SystemExit("missing config option(s): " + ", ".join(missing))


# --------------------------------------------------------------------------- errors


class S3Error(Exception):
    def __init__(self, status: int, code: str, message: str = ""):
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message = status, code, message or code


class DrimeError(Exception):
    def __init__(self, status: int, message: str):
        where = f"HTTP {status}" if status else "connection error"
        super().__init__(f"Drime {where}: {message}")
        self.status, self.message = status, message


def _err_text(data: bytes) -> str:
    try:
        j = json.loads(data)
        if isinstance(j, dict) and j.get("message"):
            return str(j["message"])
    except ValueError:
        pass
    return data[:300].decode("utf-8", "replace")


# --------------------------------------------------------------------------- http client


class RateLimiter:
    """Token bucket shared by all threads (Drime allows ~2000 API requests/min)."""

    def __init__(self, per_minute: int):
        self.rate = max(1, per_minute) / 60.0
        self.capacity = max(1.0, self.rate * 2)
        self.tokens = self.capacity
        self.t = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.t) * self.rate)
                self.t = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            time.sleep(wait)


class HTTP:
    """Keep-alive connections per thread and host; redirects drop auth across hosts."""

    def __init__(self, timeout: float):
        self.timeout = timeout
        self.local = threading.local()
        self.ctx = ssl.create_default_context()

    def _pool(self) -> dict:
        pool = getattr(self.local, "pool", None)
        if pool is None:
            pool = self.local.pool = {}
        return pool

    def _conn(self, scheme: str, netloc: str) -> http.client.HTTPConnection:
        pool = self._pool()
        conn = pool.get((scheme, netloc))
        if conn is None:
            if scheme == "https":
                conn = http.client.HTTPSConnection(netloc, timeout=self.timeout, context=self.ctx)
            else:
                conn = http.client.HTTPConnection(netloc, timeout=self.timeout)
            pool[(scheme, netloc)] = conn
        return conn

    def _drop(self, scheme: str, netloc: str) -> None:
        conn = self._pool().pop((scheme, netloc), None)
        if conn is not None:
            conn.close()

    def request(self, method, url, body=None, headers=None, max_redirects=5):
        headers = dict(headers or {})
        for _ in range(max_redirects + 1):
            u = urllib.parse.urlsplit(url)
            target = (u.path or "/") + (("?" + u.query) if u.query else "")
            conn = self._conn(u.scheme, u.netloc)
            try:
                conn.request(method, target, body=body, headers=headers)
                resp = conn.getresponse()
                data = resp.read()
            except (OSError, http.client.HTTPException):
                self._drop(u.scheme, u.netloc)
                raise
            if resp.will_close:
                self._drop(u.scheme, u.netloc)
            location = resp.getheader("Location")
            if resp.status in (301, 302, 303, 307, 308) and location:
                new = urllib.parse.urljoin(url, location)
                if urllib.parse.urlsplit(new).netloc != u.netloc:
                    headers = {k: v for k, v in headers.items() if k.lower() != "authorization"}
                if resp.status == 303 or (resp.status in (301, 302) and method not in ("GET", "HEAD")):
                    method, body = "GET", None
                    headers = {k: v for k, v in headers.items() if k.lower() != "content-type"}
                url = new
                continue
            return resp.status, {k.lower(): v for k, v in resp.getheaders()}, data
        raise DrimeError(0, "too many redirects")


# --------------------------------------------------------------------------- drime api


class Drime:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.api = cfg.drime_api.rstrip("/")
        u = urllib.parse.urlsplit(self.api)
        self.site = f"{u.scheme}://{u.netloc}/"
        self.http = HTTP(cfg.http_timeout)
        self.limiter = RateLimiter(cfg.requests_per_minute)
        self.ws = cfg.workspace_id

    def backoff(self, attempt: int, retry_after: str | None = None) -> None:
        delay = min(60.0, 0.5 * 2 ** attempt) * (0.75 + random.random() / 2)
        if retry_after:
            try:
                delay = max(delay, min(300.0, float(retry_after)))
            except ValueError:
                pass
        time.sleep(delay)

    def call(self, method, path="", *, url=None, params=None, json_body=None, body=None,
             headers=None, auth=True, rate_limited=True, retry="all", raw=False):
        """retry='all' for idempotent requests, 'throttle' retries only on 429."""
        if url is None:
            url = self.api + path
            if params:
                url += "?" + urllib.parse.urlencode(params, doseq=True)
        hdrs = {"Accept": "application/json"}
        if auth:
            hdrs["Authorization"] = "Bearer " + self.cfg.drime_token
        if json_body is not None:
            body = json.dumps(json_body).encode()
            hdrs["Content-Type"] = "application/json"
        hdrs.update(headers or {})
        attempt = 0
        while True:
            attempt += 1
            if rate_limited:
                self.limiter.acquire()
            try:
                status, rh, data = self.http.request(method, url, body, hdrs)
            except (OSError, http.client.HTTPException) as e:
                if retry == "all" and attempt <= self.cfg.retries:
                    log.debug("drime %s %s: %s, retrying", method, path or "<url>", e)
                    self.backoff(attempt)
                    continue
                raise DrimeError(0, f"{type(e).__name__}: {e}") from e
            if status in RETRY_STATUS and attempt <= self.cfg.retries and (retry == "all" or status == 429):
                log.debug("drime %s %s: HTTP %s, retrying", method, path or "<url>", status)
                self.backoff(attempt, rh.get("retry-after"))
                continue
            if status >= 400:
                raise DrimeError(status, _err_text(data))
            if raw:
                return status, rh, data
            if not data:
                return {}
            try:
                return json.loads(data)
            except ValueError as e:
                raise DrimeError(status, f"invalid JSON response: {data[:200]!r}") from e

    def _ws_params(self) -> dict:
        return {"workspaceId": self.ws} if self.ws else {}

    # -- account
    def me(self):
        return self.call("GET", "/users/me")

    def space_usage(self):
        return self.call("GET", "/user/space-usage")

    # -- folders / listing
    def list_children(self, folder_id: int | None, folders_only: bool = False):
        page = 1
        while True:
            params = {"perPage": LIST_PAGE, "page": page, **self._ws_params()}
            if folder_id is not None:
                params["folderId"] = folder_id
            if folders_only:
                params["type"] = "folder"
            res = self.call("GET", "/drive/file-entries", params=params)
            cur = int(res.get("current_page") or page)
            if cur != page:
                raise DrimeError(0, f"folder {folder_id} has more entries than Drime will list")
            for item in res.get("data") or []:
                if folders_only and item.get("type") != "folder":
                    continue
                yield item
            if cur >= int(res.get("last_page") or cur):
                return
            page += 1

    def find_folder(self, parent_id: int | None, name: str):
        for item in self.list_children(parent_id, folders_only=True):
            if item.get("name") == name:
                return item
        return None

    def create_folder(self, name: str, parent_id: int | None):
        body = {"name": name}
        if parent_id is not None:
            body["parentId"] = parent_id
        res = self.call("POST", "/folders", params=self._ws_params() or None, json_body=body,
                        retry="throttle")
        return res["folder"]

    # -- uploads
    def upload(self, parent_id: int | None, name: str, data: bytes):
        if len(data) <= SMALL_UPLOAD_MAX:
            return self.upload_small(parent_id, name, data)
        return self.upload_multipart(parent_id, name, data)

    def upload_small(self, parent_id, name, data):
        boundary = "drime-s3gw-" + uuid.uuid4().hex
        out = []

        def field(n, v):
            out.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{n}"\r\n\r\n{v}\r\n'.encode())

        field("parentId", "" if parent_id is None else str(parent_id))
        field("relativePath", name)
        field("workspaceId", str(self.ws) if self.ws else "")
        out.append(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\n'
                   f"Content-Type: application/octet-stream\r\n\r\n".encode())
        out.append(data)
        out.append(f"\r\n--{boundary}--\r\n".encode())
        res = self.call("POST", "/uploads", body=b"".join(out), retry="throttle",
                        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
        return res["fileEntry"]

    def upload_multipart(self, parent_id, name, data):
        ext = os.path.splitext(name)[1].lstrip(".")
        common = {"relativePath": name, **self._ws_params()}
        if parent_id is not None:
            common["parentId"] = parent_id
        create = self.call("POST", "/s3/multipart/create", retry="throttle", json_body={
            "filename": name, "mime": "application/octet-stream", "size": len(data),
            "extension": ext, **common})
        key, upload_id = create["key"], create["uploadId"]
        try:
            count = (len(data) + PART_SIZE - 1) // PART_SIZE
            urls = self._sign_parts(key, upload_id, list(range(1, count + 1)))
            parts = []
            for pn in range(1, count + 1):
                chunk = data[(pn - 1) * PART_SIZE: pn * PART_SIZE]
                parts.append({"PartNumber": pn, "ETag": self._put_part(key, upload_id, pn, urls, chunk)})
            self.call("POST", "/s3/multipart/complete", retry="throttle",
                      json_body={"key": key, "uploadId": upload_id, "parts": parts})
        except BaseException:
            try:
                self.call("POST", "/s3/multipart/abort", json_body={"key": key, "uploadId": upload_id})
            except Exception as e:  # noqa: BLE001
                log.warning("could not abort multipart upload %s: %s", upload_id, e)
            raise
        res = self.call("POST", "/s3/entries", retry="throttle", json_body={
            "clientMime": "application/octet-stream", "clientName": name,
            "filename": key.rsplit("/", 1)[-1], "size": len(data),
            "clientExtension": ext or "bin", **common})
        return res["fileEntry"]

    def _sign_parts(self, key, upload_id, numbers):
        urls = {}
        for i in range(0, len(numbers), 100):
            res = self.call("POST", "/s3/multipart/batch-sign-part-urls", json_body={
                "key": key, "uploadId": upload_id, "partNumbers": numbers[i:i + 100]})
            for u in res.get("urls") or []:
                urls[int(u["partNumber"])] = u["url"]
        missing = [n for n in numbers if n not in urls]
        if missing:
            raise DrimeError(0, f"Drime did not sign part(s) {missing[:5]}")
        return urls

    def _put_part(self, key, upload_id, pn, urls, chunk):
        for attempt in (1, 2, 3):
            try:
                status, rh, _ = self.call("PUT", url=urls[pn], body=chunk, auth=False,
                                          rate_limited=False, raw=True,
                                          headers={"Content-Type": "application/octet-stream"})
            except DrimeError as e:
                if e.status == 403 and attempt < 3:  # signed URL expired: sign again
                    urls.update(self._sign_parts(key, upload_id, [pn]))
                    continue
                raise
            etag = rh.get("etag")
            if not etag:
                raise DrimeError(status, f"part {pn} upload returned no ETag")
            return etag if etag.startswith('"') else f'"{etag}"'
        raise DrimeError(0, "unreachable")

    # -- download / delete / rename
    def download(self, item_url: str | None, item_hash: str | None) -> bytes:
        urls = []
        if item_url:
            urls.append(item_url if "://" in item_url else self.site + item_url.lstrip("/"))
        if item_hash:
            urls.append(f"{self.api}/file-entries/download/{item_hash}")
        if not urls:
            raise DrimeError(0, "entry has neither url nor hash")
        for i, url in enumerate(urls):
            try:
                return self.call("GET", url=url, raw=True)[2]
            except DrimeError as e:
                if e.status in (404, 410) and i + 1 < len(urls):
                    continue
                raise
        raise DrimeError(0, "unreachable")

    def delete(self, ids: list[int], forever: bool) -> None:
        for i in range(0, len(ids), 100):
            batch = ids[i:i + 100]
            self._delete_batch(batch, False)
            if forever:
                self._delete_batch(batch, True)

    def _delete_batch(self, ids, forever):
        try:
            res = self.call("POST", "/file-entries/delete",
                            json_body={"entryIds": ids, "deleteForever": forever})
        except DrimeError as e:
            if e.status == 422 and "invalid" in e.message.lower():
                if len(ids) > 1:  # find out which ones still exist
                    for one in ids:
                        self._delete_batch([one], forever)
                return  # already gone
            raise
        errors = res.get("errors") if isinstance(res, dict) else None
        if errors:
            raise DrimeError(200, f"delete reported errors: {errors}")

    def rename(self, entry_id: int, name: str):
        return self.call("POST", f"/file-entries/{entry_id}", params=self._ws_params() or None,
                         json_body={"name": name}, headers={"X-HTTP-Method-Override": "PUT"})


# --------------------------------------------------------------------------- index


@dataclass
class Obj:
    key: str
    size: int
    etag: str
    mtime: float
    entry_id: int
    url: str | None
    hash: str | None


SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    key TEXT PRIMARY KEY, size INTEGER NOT NULL, etag TEXT NOT NULL, mtime REAL NOT NULL,
    entry_id INTEGER NOT NULL, url TEXT, hash TEXT) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS folders (path TEXT PRIMARY KEY, id INTEGER NOT NULL) WITHOUT ROWID;
"""


class Index:
    """Local SQLite index: S3 key -> Drime entry. Rebuildable from Drime (rebuild-index)."""

    def __init__(self, path: str):
        self.path = path
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
        self.local = threading.local()
        self._c().executescript(SCHEMA)

    def _c(self) -> sqlite3.Connection:
        c = getattr(self.local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.path, timeout=60, isolation_level=None)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=FULL")
            self.local.conn = c
        return c

    def get(self, key: str) -> Obj | None:
        r = self._c().execute("SELECT key,size,etag,mtime,entry_id,url,hash FROM objects WHERE key=?",
                              (key,)).fetchone()
        return Obj(*r) if r else None

    def put(self, o: Obj) -> None:
        self._c().execute("INSERT OR REPLACE INTO objects VALUES (?,?,?,?,?,?,?)",
                          (o.key, o.size, o.etag, o.mtime, o.entry_id, o.url, o.hash))

    def delete_if(self, key: str, entry_id: int) -> None:
        self._c().execute("DELETE FROM objects WHERE key=? AND entry_id=?", (key, entry_id))

    def list_after(self, prefix: str, cursor: str, limit: int) -> list[Obj]:
        sql = "SELECT key,size,etag,mtime,entry_id,url,hash FROM objects WHERE key > ? AND key >= ?"
        args: list = [cursor, prefix]
        if prefix:
            sql += " AND key < ?"
            args.append(prefix + TOP)
        sql += " ORDER BY key LIMIT ?"
        args.append(limit)
        return [Obj(*r) for r in self._c().execute(sql, args)]

    def stats(self) -> tuple[int, int]:
        n, s = self._c().execute("SELECT count(*), coalesce(sum(size),0) FROM objects").fetchone()
        return n, s

    def get_folder(self, path: str) -> int | None:
        r = self._c().execute("SELECT id FROM folders WHERE path=?", (path,)).fetchone()
        return r[0] if r else None

    def set_folder(self, path: str, fid: int) -> None:
        self._c().execute("INSERT OR REPLACE INTO folders VALUES (?,?)", (path, fid))

    def forget_folders(self, path: str) -> None:
        self._c().execute("DELETE FROM folders WHERE path=? OR (path > ? AND path < ?)",
                          (path, path + "/", path + "/" + TOP))

    def clear(self) -> None:
        self._c().executescript("DELETE FROM objects; DELETE FROM folders;")


# --------------------------------------------------------------------------- key <-> drime layout


def key_to_location(key: str) -> tuple[tuple[str, ...], str]:
    """Map an S3 key to (folder path, file name) on Drime.

    PBS chunk keys `<store>/.chunks/<4hex>/<digest>` are stored as
    `<store>/.chunks/<2hex>/<2hex>/<digest>` so no Drime folder ever needs to list more
    than 256 sub-folders (Drime caps folder listings at ~20k entries).
    """
    parts = key.split("/")
    for p in parts:
        if p in ("", ".", "..") or p != p.strip() or "\\" in p or '"' in p \
                or any(ord(ch) < 32 for ch in p) or len(p.encode()) > 200:
            raise S3Error(400, "InvalidArgument", f"unsupported object key {key!r}")
    if parts[-1].endswith(EMPTY_SUFFIX):
        raise S3Error(400, "InvalidArgument", f"object keys must not end in {EMPTY_SUFFIX}")
    folders, name = parts[:-1], parts[-1]
    if len(parts) >= 3 and parts[-3] == ".chunks" and HEX4.fullmatch(parts[-2]):
        folders = parts[:-2] + [parts[-2][:2], parts[-2][2:]]
    if location_to_key(folders, name) != key:
        raise S3Error(400, "InvalidArgument", f"object key {key!r} collides with the chunk layout")
    return tuple(folders), name


def location_to_key(folders, name: str) -> str:
    f = list(folders)
    if len(f) >= 3 and f[-3] == ".chunks" and HEX2.fullmatch(f[-2]) and HEX2.fullmatch(f[-1]):
        f = f[:-2] + [f[-2] + f[-1]]
    return "/".join(f + [name])


def _is_md5_etag(etag: str) -> bool:
    return bool(re.fullmatch(r'"[0-9a-f]{32}"', etag))


# --------------------------------------------------------------------------- store


class Store:
    def __init__(self, cfg: Config, drime: Drime, index: Index):
        self.cfg, self.drime, self.index = cfg, drime, index
        self.root_parts = tuple(p for p in cfg.drime_root.split("/") if p)
        if not self.root_parts:
            raise SystemExit("drime_root must name a folder, e.g. 'pbs-s3'")
        self.folder_lock = threading.RLock()
        self.fresh: set[int] = set()  # folders created by this process (known to start empty)
        self.key_locks = [threading.Lock() for _ in range(512)]
        self.inflight = threading.BoundedSemaphore(max(1, cfg.max_inflight))

    def _key_lock(self, key: str) -> threading.Lock:
        return self.key_locks[int.from_bytes(hashlib.blake2s(key.encode(), digest_size=4).digest()) % 512]

    # -- folders
    def folder_id(self, rel) -> int:
        return self._folder(self.root_parts + tuple(rel))

    def _folder(self, full: tuple[str, ...]) -> int:
        path = "/".join(full)
        fid = self.index.get_folder(path)
        if fid is not None:
            return fid
        with self.folder_lock:
            fid = self.index.get_folder(path)
            if fid is not None:
                return fid
            parent = self._folder(full[:-1]) if len(full) > 1 else None
            name = full[-1]
            if parent is None or parent not in self.fresh:
                hit = self.drime.find_folder(parent, name)
                if hit:
                    fid = int(hit["id"])
            if fid is None:
                fid = self._create_folder(parent, name)
                self.fresh.add(fid)
            self.index.set_folder(path, fid)
            return fid

    def _create_folder(self, parent, name) -> int:
        attempt = 0
        while True:
            attempt += 1
            try:
                return int(self.drime.create_folder(name, parent)["id"])
            except DrimeError as e:
                if (e.status and e.status < 500) or attempt > self.cfg.retries:
                    raise
                log.warning("creating folder %r failed (%s); checking whether it exists", name, e)
                self.drime.backoff(attempt)
                hit = self.drime.find_folder(parent, name)
                if hit:
                    return int(hit["id"])

    # -- uploads
    def _upload(self, folders, dname: str, payload: bytes, exclude: set[int]):
        md5 = hashlib.md5(payload).digest()
        attempt = 0
        stale_folder_retry = True
        while True:
            attempt += 1
            pid = self.folder_id(folders)
            try:
                return self.drime.upload(pid, dname, payload)
            except DrimeError as e:
                if e.status in (404, 422) and stale_folder_retry and "parent" in e.message.lower():
                    log.warning("Drime rejected parent folder of %s (%s); re-resolving", dname, e)
                    self.index.forget_folders("/".join(self.root_parts + tuple(folders)))
                    stale_folder_retry = False
                    continue
                ambiguous = e.status == 0 or e.status >= 500
                if not ambiguous or attempt > self.cfg.retries:
                    raise
                log.warning("upload of %s failed (%s); checking whether it landed anyway", dname, e)
                self.drime.backoff(attempt)
                landed = self._find_uploaded(pid, dname, payload, md5, exclude)
                if landed:
                    return landed

    def _find_uploaded(self, pid, dname, payload, md5, exclude):
        pat = re.compile(re.escape(dname) + r"( \(\d+\))?(\.bin)?")
        for item in self.drime.list_children(pid):
            if item.get("type") == "folder" or int(item["id"]) in exclude:
                continue
            if not pat.fullmatch(item.get("name") or "") or int(item.get("file_size") or -1) != len(payload):
                continue
            try:
                data = self.drime.download(item.get("url"), item.get("hash"))
            except DrimeError:
                continue
            if hashlib.md5(data).digest() == md5:
                log.info("found completed upload of %s (entry %s)", dname, item["id"])
                return item
        return None

    # -- S3 operations
    def head(self, key: str) -> Obj:
        o = self.index.get(key)
        if o is None:
            raise S3Error(404, "NoSuchKey", "The specified key does not exist.")
        return o

    def put(self, key: str, data: bytes, if_none_match: bool = False) -> Obj:
        folders, name = key_to_location(key)
        etag = f'"{hashlib.md5(data).hexdigest()}"'
        with self._key_lock(key):
            old = self.index.get(key)
            if old is not None and if_none_match:
                raise S3Error(412, "PreconditionFailed",
                              "At least one of the pre-conditions you specified did not hold")
            dname, payload = (name + EMPTY_SUFFIX, b"\0") if not data else (name, data)
            with self.inflight:
                item = self._upload(folders, dname, payload, {old.entry_id} if old else set())
            obj = Obj(key, len(data), etag, time.time(), int(item["id"]), item.get("url"), item.get("hash"))
            self.index.put(obj)
            if old is not None and old.entry_id != obj.entry_id:
                try:
                    self.drime.delete([old.entry_id], self.cfg.hard_delete)
                except DrimeError as e:
                    log.warning("could not delete replaced entry %s of %s: %s", old.entry_id, key, e)
            if item.get("name") != dname:
                try:
                    self.drime.rename(obj.entry_id, dname)
                except DrimeError as e:
                    log.warning("Drime stored %s as %r and rename failed: %s", key, item.get("name"), e)
        return obj

    def get(self, key: str) -> tuple[Obj, bytes]:
        o = self.head(key)
        if o.size == 0:
            return o, b""
        problem = ""
        with self.inflight:
            for _ in range(3):
                try:
                    data = self.drime.download(o.url, o.hash)
                except DrimeError as e:
                    if e.status in (404, 410):
                        log.error("Drime has no data for %s (entry %s)", key, o.entry_id)
                        raise S3Error(404, "NoSuchKey", "object data missing on Drime") from e
                    raise
                if len(data) != o.size:
                    problem = f"got {len(data)} of {o.size} bytes"
                elif _is_md5_etag(o.etag) and f'"{hashlib.md5(data).hexdigest()}"' != o.etag:
                    problem = "MD5 mismatch"
                else:
                    return o, data
                log.warning("download of %s from Drime: %s, retrying", key, problem)
        raise S3Error(500, "InternalError", f"download of {key} failed integrity check: {problem}")

    def delete(self, keys: list[str]) -> None:
        objs = [o for o in (self.index.get(k) for k in keys) if o is not None]
        if not objs:
            return
        with self.inflight:
            self.drime.delete([o.entry_id for o in objs], self.cfg.hard_delete)
        for o in objs:
            with self._key_lock(o.key):
                self.index.delete_if(o.key, o.entry_id)

    def copy(self, src: str, dst: str) -> Obj:
        _, data = self.get(src)
        return self.put(dst, data)

    def list(self, prefix: str, cursor: str, max_keys: int, delimiter: str):
        """Returns (objects, common_prefixes, truncated, next_cursor)."""
        contents: list[Obj] = []
        prefixes: list[str] = []
        truncated = False
        while True:
            rows = self.index.list_after(prefix, cursor, LIST_PAGE)
            if not rows:
                break
            requery = False
            for o in rows:
                if len(contents) + len(prefixes) >= max_keys:
                    truncated = True
                    break
                if delimiter:
                    i = o.key.find(delimiter, len(prefix))
                    if i >= 0:
                        cp = o.key[: i + len(delimiter)]
                        prefixes.append(cp)
                        cursor = cp + TOP
                        requery = True
                        break
                contents.append(o)
                cursor = o.key
            if truncated or (not requery and len(rows) < LIST_PAGE):
                break
        return contents, prefixes, truncated, cursor

    # -- maintenance
    def rebuild_index(self) -> dict:
        root = self.folder_id(())
        self.index.clear()
        stats = {"objects": 0, "bytes": 0, "folders": 0, "duplicates": 0}
        seen: dict[str, float] = {}
        stack: list[tuple[tuple[str, ...], int]] = [((), root)]
        while stack:
            rel, fid = stack.pop()
            self.index.set_folder("/".join(self.root_parts + rel), fid)
            stats["folders"] += 1
            for item in self.drime.list_children(fid):
                name = item.get("name") or ""
                if item.get("type") == "folder":
                    stack.append((rel + (name,), int(item["id"])))
                    continue
                size = int(item.get("file_size") or 0)
                if name.endswith(EMPTY_SUFFIX):
                    name, size = name[: -len(EMPTY_SUFFIX)], 0
                key = location_to_key(rel, name)
                mtime = _parse_time(item.get("updated_at") or item.get("created_at"))
                if key in seen:
                    stats["duplicates"] += 1
                    log.warning("duplicate Drime entries for %s; keeping the newest", key)
                    if mtime < seen[key]:
                        continue
                else:
                    stats["objects"] += 1
                    stats["bytes"] += size
                seen[key] = mtime
                self.index.put(Obj(key, size, f'"drime-{item["id"]}"', mtime, int(item["id"]),
                                   item.get("url"), item.get("hash")))
        return stats


def _parse_time(s) -> float:
    if not s:
        return time.time()
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return time.time()


# --------------------------------------------------------------------------- sigv4


def _aws_quote(s: str) -> str:
    return urllib.parse.quote(s, safe="-_.~")


def parse_query(raw: str) -> list[tuple[str, str]]:
    pairs = []
    for part in raw.split("&"):
        if part:
            k, _, v = part.partition("=")
            pairs.append((urllib.parse.unquote_plus(k), urllib.parse.unquote_plus(v)))
    return pairs


class SigV4:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._keys: dict[tuple[str, str], bytes] = {}

    def _signing_key(self, date: str, region: str) -> bytes:
        k = self._keys.get((date, region))
        if k is None:
            k = ("AWS4" + self.cfg.secret_key).encode()
            for part in (date, region, "s3", "aws4_request"):
                k = hmac.new(k, part.encode(), hashlib.sha256).digest()
            if len(self._keys) > 64:
                self._keys.clear()
            self._keys[(date, region)] = k
        return k

    def verify(self, method: str, raw_path: str, raw_query: str, headers, body: bytes) -> None:
        auth = headers.get("Authorization")
        if not auth:
            raise S3Error(403, "AccessDenied", "anonymous access is not allowed")
        scheme = "AWS4-HMAC-SHA256 "
        if not auth.startswith(scheme):
            raise S3Error(400, "AuthorizationHeaderMalformed", "only AWS4-HMAC-SHA256 is supported")
        try:
            parts = dict(p.strip().split("=", 1) for p in auth[len(scheme):].split(","))
            access_key, date, region, service, term = parts["Credential"].split("/")
            signed = parts["SignedHeaders"]
            signature = parts["Signature"]
        except (KeyError, ValueError):
            raise S3Error(400, "AuthorizationHeaderMalformed", "malformed Authorization header") from None
        if not hmac.compare_digest(access_key, self.cfg.access_key):
            raise S3Error(403, "InvalidAccessKeyId", "unknown access key")
        if service != "s3" or term != "aws4_request":
            raise S3Error(400, "AuthorizationHeaderMalformed", "bad credential scope")
        if region != self.cfg.region:
            raise S3Error(400, "AuthorizationHeaderMalformed",
                          f"region {region!r} is wrong; expecting {self.cfg.region!r}")
        amz_date = headers.get("x-amz-date") or ""
        try:
            t = datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            raise S3Error(403, "AccessDenied", "missing or malformed x-amz-date") from None
        if abs(time.time() - t.timestamp()) > 15 * 60:
            raise S3Error(403, "RequestTimeTooSkewed", "request time too far from server time")
        if amz_date[:8] != date:
            raise S3Error(400, "AuthorizationHeaderMalformed", "credential date mismatch")
        payload = headers.get("x-amz-content-sha256") or ""
        if payload.startswith("STREAMING-"):
            raise S3Error(501, "NotImplemented", "aws-chunked uploads are not supported")
        if payload != "UNSIGNED-PAYLOAD":
            if not hmac.compare_digest(hashlib.sha256(body).hexdigest(), payload.lower()):
                raise S3Error(400, "XAmzContentSHA256Mismatch", "payload hash mismatch")
        names = signed.split(";")
        if "host" not in names:
            raise S3Error(403, "AccessDenied", "host header must be signed")
        lines = []
        for name in names:
            values = headers.get_all(name)
            if values is None:
                raise S3Error(403, "SignatureDoesNotMatch", f"signed header {name} missing")
            lines.append(f"{name}:" + ",".join(" ".join(v.split()) for v in values))
        query = "&".join(sorted(f"{_aws_quote(k)}={_aws_quote(v)}" for k, v in parse_query(raw_query)))
        canonical = "\n".join([method, raw_path or "/", query, "\n".join(lines), "", signed, payload])
        scope = f"{date}/{region}/s3/aws4_request"
        sts = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope,
                         hashlib.sha256(canonical.encode()).hexdigest()])
        expected = hmac.new(self._signing_key(date, region), sts.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            log.debug("signature mismatch; canonical request was:\n%s", canonical)
            raise S3Error(403, "SignatureDoesNotMatch",
                          "The request signature we calculated does not match the signature you provided.")


# --------------------------------------------------------------------------- s3 http frontend


def _x(s) -> str:
    return _xml_escape(str(s))


def _iso(t: float) -> str:
    d = datetime.fromtimestamp(t, timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S.") + f"{d.microsecond // 1000:03d}Z"


def _http_date(t: float) -> str:
    return email.utils.formatdate(t, usegmt=True)


def _xml(body: str) -> bytes:
    return ('<?xml version="1.0" encoding="UTF-8"?>\n' + body).encode()


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = f"drime-s3gw/{__version__}"
    sys_version = ""
    timeout = 300  # idle keep-alive / slow client timeout
    store: Store
    auth: SigV4
    cfg: Config

    def setup(self):
        if isinstance(self.request, ssl.SSLSocket):
            self.request.settimeout(30)
            self.request.do_handshake()
        super().setup()

    def log_message(self, fmt, *args):
        log.debug("%s " + fmt, self.address_string(), *args)

    def do_GET(self):
        self._dispatch("GET")

    def do_HEAD(self):
        self._dispatch("HEAD")

    def do_PUT(self):
        self._dispatch("PUT")

    def do_POST(self):
        self._dispatch("POST")

    def do_DELETE(self):
        self._dispatch("DELETE")

    # -- plumbing
    def _send(self, status: int, body: bytes = b"", headers: dict | None = None, ctype: str | None = None):
        self.send_response(status)
        self.send_header("x-amz-request-id", self._rid)
        hdrs = dict(headers or {})
        if ctype:
            hdrs["Content-Type"] = ctype
        if status not in (204, 304) and "Content-Length" not in hdrs:
            hdrs["Content-Length"] = str(len(body))
        for k, v in hdrs.items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD" and body and status not in (204, 304):
            self.wfile.write(body)

    def _send_error(self, e: S3Error):
        body = b"" if self.command == "HEAD" else _xml(
            f"<Error><Code>{_x(e.code)}</Code><Message>{_x(e.message)}</Message>"
            f"<Resource>{_x(self.path.split('?')[0])}</Resource><RequestId>{self._rid}</RequestId></Error>")
        self._send(e.status, body, ctype="application/xml")

    def _read_body(self) -> bytes:
        te = self.headers.get("Transfer-Encoding")
        if te and te.lower() != "identity":
            self.close_connection = True
            raise S3Error(411, "MissingContentLength", "chunked transfer encoding is not supported")
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            self.close_connection = True
            raise S3Error(400, "InvalidArgument", "bad Content-Length") from None
        if n > self.cfg.max_object_size + MiB:
            self.close_connection = True
            raise S3Error(400, "EntityTooLarge", "object too large")
        data = self.rfile.read(n) if n else b""
        if len(data) != n:
            self.close_connection = True
            raise S3Error(400, "IncompleteBody", "request body shorter than Content-Length")
        return data

    def _dispatch(self, method: str):
        self._rid = uuid.uuid4().hex[:16].upper()
        t0 = time.monotonic()
        status = "?"
        try:
            raw_path, _, raw_query = self.path.partition("?")
            body = self._read_body()
            self.auth.verify(method, raw_path, raw_query, self.headers, body)
            status = self._route(method, raw_path, dict(parse_query(raw_query)), body)
        except S3Error as e:
            status = e.status
            if e.status >= 500 or e.status in (400, 403, 501):
                log.warning("%s %s -> %s %s", method, self.path, e.status, e.message)
            self._send_error(e)
        except DrimeError as e:
            status = 503
            log.error("%s %s -> Drime failure: %s", method, self.path, e)
            self._send_error(S3Error(503, "SlowDown" if e.status == 429 else "ServiceUnavailable", str(e)))
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
            return
        except Exception:  # noqa: BLE001
            status = 500
            log.exception("%s %s failed", method, self.path)
            self._send_error(S3Error(500, "InternalError", "internal error"))
        log.debug("%s %s -> %s (%.0f ms)", method, self.path, status, (time.monotonic() - t0) * 1000)

    # -- routing
    def _route(self, method: str, raw_path: str, q: dict, body: bytes) -> int:
        path = urllib.parse.unquote(raw_path)
        if not path.startswith("/"):
            raise S3Error(400, "InvalidURI", "bad path")
        bucket, _, key = path[1:].partition("/")
        if not bucket:
            if method == "GET":
                return self._list_buckets()
            raise S3Error(405, "MethodNotAllowed", "method not allowed")
        if bucket != self.cfg.bucket:
            raise S3Error(404, "NoSuchBucket", "The specified bucket does not exist")
        if not key:
            if method == "HEAD":
                self._send(200)
                return 200
            if method == "GET" and "location" in q:
                self._send(200, _xml(f'<LocationConstraint xmlns="{S3_NS}">{_x(self.cfg.region)}'
                                     "</LocationConstraint>"), ctype="application/xml")
                return 200
            if method == "GET":
                return self._list(q)
            if method == "POST" and "delete" in q:
                return self._delete_objects(body)
            if method == "PUT" and not q:
                self._send(200)  # CreateBucket on the existing bucket
                return 200
            raise S3Error(501, "NotImplemented", "bucket operation not supported")
        for sub in ("uploads", "uploadId", "partNumber", "tagging", "acl", "versionId", "attributes",
                    "retention", "legal-hold", "torrent"):
            if sub in q:
                raise S3Error(501, "NotImplemented", f"?{sub} is not supported")
        if method == "PUT":
            if self.headers.get("x-amz-copy-source"):
                return self._copy(key)
            return self._put(key, body)
        if method in ("GET", "HEAD"):
            return self._get(key, head=method == "HEAD")
        if method == "DELETE":
            self.store.delete([key])
            self._send(204)
            return 204
        raise S3Error(405, "MethodNotAllowed", "method not allowed")

    def _check_md5(self, body: bytes):
        cmd5 = self.headers.get("Content-MD5")
        if cmd5 is None:
            return
        try:
            want = base64.b64decode(cmd5.strip(), validate=True)
        except ValueError:
            raise S3Error(400, "InvalidDigest", "Content-MD5 is not valid base64") from None
        if want != hashlib.md5(body).digest():
            raise S3Error(400, "BadDigest", "Content-MD5 does not match the body")

    def _put(self, key: str, body: bytes) -> int:
        self._check_md5(body)
        if self.headers.get("If-Match"):
            raise S3Error(501, "NotImplemented", "If-Match is not supported")
        inm = (self.headers.get("If-None-Match") or "").strip()
        if inm not in ("", "*"):
            raise S3Error(501, "NotImplemented", "only If-None-Match: * is supported")
        if len(body) > self.cfg.max_object_size:
            raise S3Error(400, "EntityTooLarge", "object too large")
        obj = self.store.put(key, body, if_none_match=inm == "*")
        self._send(200, headers={"ETag": obj.etag})
        return 200

    def _get(self, key: str, head: bool) -> int:
        if head:
            o, data = self.store.head(key), b""
        else:
            o, data = self.store.get(key)
        hdrs = {"ETag": o.etag, "Last-Modified": _http_date(o.mtime), "Accept-Ranges": "bytes",
                "Content-Type": "application/octet-stream"}
        rng = self.headers.get("Range")
        m = re.fullmatch(r"bytes=(\d*)-(\d*)", (rng or "").strip())
        if m and not head and (m.group(1) or m.group(2)):
            a, b = m.group(1), m.group(2)
            if a:
                start, end = int(a), min(int(b), o.size - 1) if b else o.size - 1
            else:
                start, end = max(0, o.size - int(b)), o.size - 1
            if start >= o.size or start > end:
                hdrs.pop("Content-Type")
                raise S3Error(416, "InvalidRange", "The requested range is not satisfiable")
            hdrs["Content-Range"] = f"bytes {start}-{end}/{o.size}"
            self._send(206, data[start:end + 1], hdrs)
            return 206
        if head:
            hdrs["Content-Length"] = str(o.size)
        self._send(200, data, hdrs)
        return 200

    def _copy(self, dst: str) -> int:
        src = urllib.parse.unquote(self.headers["x-amz-copy-source"]).split("?", 1)[0].lstrip("/")
        sbucket, _, skey = src.partition("/")
        if sbucket != self.cfg.bucket or not skey:
            raise S3Error(400, "InvalidArgument", "copy source must be in the same bucket")
        obj = self.store.copy(skey, dst)
        self._send(200, _xml(f'<CopyObjectResult xmlns="{S3_NS}"><LastModified>{_iso(obj.mtime)}'
                             f"</LastModified><ETag>{_x(obj.etag)}</ETag></CopyObjectResult>"),
                   ctype="application/xml")
        return 200

    def _delete_objects(self, body: bytes) -> int:
        self._check_md5(body)
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            raise S3Error(400, "MalformedXML", "invalid Delete XML") from None
        quiet = False
        keys = []
        for el in root:
            tag = _local(el.tag)
            if tag == "Quiet":
                quiet = (el.text or "").strip().lower() == "true"
            elif tag == "Object":
                for sub in el:
                    if _local(sub.tag) == "Key" and sub.text:
                        keys.append(sub.text)
        if len(keys) > 1000:
            raise S3Error(400, "MalformedXML", "at most 1000 keys per request")
        out = [f'<DeleteResult xmlns="{S3_NS}">']
        try:
            self.store.delete(keys)
            if not quiet:
                out += [f"<Deleted><Key>{_x(k)}</Key></Deleted>" for k in keys]
        except DrimeError as e:
            log.error("batch delete failed: %s", e)
            out += [f"<Error><Key>{_x(k)}</Key><Code>InternalError</Code><Message>{_x(e)}</Message></Error>"
                    for k in keys]
        out.append("</DeleteResult>")
        self._send(200, _xml("".join(out)), ctype="application/xml")
        return 200

    def _list(self, q: dict) -> int:
        v2 = q.get("list-type") == "2"
        prefix = q.get("prefix", "")
        delimiter = q.get("delimiter", "")
        try:
            max_keys = max(0, min(1000, int(q.get("max-keys", "1000"))))
        except ValueError:
            raise S3Error(400, "InvalidArgument", "bad max-keys") from None
        if v2:
            token = q.get("continuation-token")
            if token:
                try:
                    cursor = base64.urlsafe_b64decode(token.encode()).decode()
                except ValueError:
                    raise S3Error(400, "InvalidArgument", "bad continuation token") from None
            else:
                cursor = q.get("start-after", "")
        else:
            token, cursor = None, q.get("marker", "")
        objs, prefixes, truncated, nxt = self.store.list(prefix, cursor, max_keys, delimiter)
        o = [f'<ListBucketResult xmlns="{S3_NS}"><Name>{_x(self.cfg.bucket)}</Name>'
             f"<Prefix>{_x(prefix)}</Prefix><MaxKeys>{max_keys}</MaxKeys>"
             f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>"]
        if delimiter:
            o.append(f"<Delimiter>{_x(delimiter)}</Delimiter>")
        if v2:
            o.append(f"<KeyCount>{len(objs) + len(prefixes)}</KeyCount>")
            if token:
                o.append(f"<ContinuationToken>{_x(token)}</ContinuationToken>")
            if "start-after" in q:
                o.append(f"<StartAfter>{_x(q['start-after'])}</StartAfter>")
            if truncated:
                tok = base64.urlsafe_b64encode(nxt.encode()).decode()
                o.append(f"<NextContinuationToken>{tok}</NextContinuationToken>")
        else:
            o.append(f"<Marker>{_x(cursor)}</Marker>")
            if truncated and delimiter:
                o.append(f"<NextMarker>{_x(nxt)}</NextMarker>")
        for ob in objs:
            o.append(f"<Contents><Key>{_x(ob.key)}</Key><LastModified>{_iso(ob.mtime)}</LastModified>"
                     f"<ETag>{_x(ob.etag)}</ETag><Size>{ob.size}</Size>"
                     "<StorageClass>STANDARD</StorageClass></Contents>")
        for cp in prefixes:
            o.append(f"<CommonPrefixes><Prefix>{_x(cp)}</Prefix></CommonPrefixes>")
        o.append("</ListBucketResult>")
        self._send(200, _xml("".join(o)), ctype="application/xml")
        return 200

    def _list_buckets(self) -> int:
        body = (f'<ListAllMyBucketsResult xmlns="{S3_NS}"><Owner><ID>drime-s3gw</ID>'
                "<DisplayName>drime-s3gw</DisplayName></Owner><Buckets><Bucket>"
                f"<Name>{_x(self.cfg.bucket)}</Name><CreationDate>2025-01-01T00:00:00.000Z</CreationDate>"
                "</Bucket></Buckets></ListAllMyBucketsResult>")
        self._send(200, _xml(body), ctype="application/xml")
        return 200


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def __init__(self, addr, handler, sslctx: ssl.SSLContext | None):
        self.sslctx = sslctx
        if ":" in addr[0]:
            self.address_family = socket.AF_INET6
        super().__init__(addr, handler)

    def get_request(self):
        sock, addr = super().get_request()
        if self.sslctx is not None:
            sock = self.sslctx.wrap_socket(sock, server_side=True, do_handshake_on_connect=False)
        return sock, addr

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (ssl.SSLError, ConnectionError, TimeoutError, socket.timeout)):
            log.debug("connection from %s: %s", client_address, exc)
            return
        log.exception("unhandled error serving %s", client_address)


def make_server(cfg: Config, store: Store) -> Server:
    handler = type("BoundHandler", (Handler,), {"store": store, "auth": SigV4(cfg), "cfg": cfg})
    ctx = None
    if cfg.tls:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(cfg.cert, cfg.key)
    return Server((cfg.listen, cfg.port), handler, ctx)


# --------------------------------------------------------------------------- cli


def _build(cfg: Config) -> Store:
    cfg.require("drime_token")
    return Store(cfg, Drime(cfg), Index(cfg.index_db))


def cmd_serve(cfg: Config, args) -> int:
    cfg.require("access_key", "secret_key", "drime_token")
    store = _build(cfg)
    store.folder_id(())  # fail fast on a bad token / unreachable Drime
    srv = make_server(cfg, store)
    n, size = store.index.stats()
    log.info("drime-s3gw %s listening on %s:%s (%s), bucket %r -> Drime folder %r, %d objects / %.1f GiB",
             __version__, cfg.listen, cfg.port, "https" if cfg.tls else "http", cfg.bucket,
             cfg.drime_root, n, size / 1024 ** 3)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


def cmd_check(cfg: Config, args) -> int:
    store = _build(cfg)
    me = store.drime.me()
    user = me.get("user", me)
    print(f"token OK: {user.get('email') or user.get('name') or user.get('id')}")
    try:
        su = store.drime.space_usage()
        print(f"space: {int(su.get('used', 0)) / 1024 ** 3:.2f} GiB used, "
              f"{int(su.get('available', 0)) / 1024 ** 3:.2f} GiB available")
    except DrimeError as e:
        print(f"space usage unavailable: {e}")
    print(f"root folder {cfg.drime_root!r}: id {store.folder_id(())}")
    n, size = store.index.stats()
    print(f"index {cfg.index_db}: {n} objects, {size / 1024 ** 3:.2f} GiB")
    return 0


def cmd_selftest(cfg: Config, args) -> int:
    """Round-trip real objects through Drime using the same code paths as the gateway."""
    store = _build(cfg)
    base = "_s3gw_selftest"
    digest = hashlib.sha256(os.urandom(16)).hexdigest()
    cases = {
        f"{base}/.chunks/{digest[:4]}/{digest}": os.urandom(300 * 1024),
        f"{base}/vm/100/2026-11-04T10:00:00Z/drive-scsi0.img.fidx": os.urandom(12 * MiB + 123),
        f"{base}/.namespace": b"",
        f"{base}/vm/100/owner": b"root@pam\n",
    }
    ok = True

    def check(cond, what):
        nonlocal ok
        print(("  ok   " if cond else "  FAIL ") + what)
        ok = ok and cond

    print("uploading ...")
    for k, v in cases.items():
        store.put(k, v)
    print("reading back ...")
    for k, v in cases.items():
        check(store.get(k)[1] == v, f"get {k} ({len(v)} bytes)")
    try:
        store.put(f"{base}/vm/100/owner", b"x", if_none_match=True)
        check(False, "If-None-Match on existing key returns 412")
    except S3Error as e:
        check(e.status == 412, "If-None-Match on existing key returns 412")
    store.put(f"{base}/vm/100/owner", b"root@pam!token\n")
    check(store.get(f"{base}/vm/100/owner")[1] == b"root@pam!token\n", "overwrite replaces content")
    print("checking names on Drime ...")
    for k, v in cases.items():
        folders, name = key_to_location(k)
        dname = name + EMPTY_SUFFIX if not v else name
        names = [i.get("name") for i in store.drime.list_children(store.folder_id(folders))
                 if i.get("type") != "folder"]
        check(names.count(dname) == 1 and len(names) == 1 if k.endswith("owner") else dname in names,
              f"Drime name {'/'.join(folders)}/{dname} (found {names})")
    objs, _, _, _ = store.list(base + "/", "", 1000, "")
    check(sorted(o.key for o in objs) == sorted(cases), "listing returns every key")
    print("deleting ...")
    store.delete(list(cases))
    objs, _, _, _ = store.list(base + "/", "", 1000, "")
    check(not objs, "index is empty after delete")
    for k in cases:
        folders, name = key_to_location(k)
        left = [i.get("name") for i in store.drime.list_children(store.folder_id(folders))
                if i.get("type") != "folder"]
        check(not left, f"Drime folder {'/'.join(folders)} is empty")
    top = store.folder_id((base,))
    store.drime.delete([top], cfg.hard_delete)
    store.index.forget_folders("/".join(store.root_parts + (base,)))
    print("PASS" if ok else "FAILED")
    return 0 if ok else 1


def cmd_rebuild(cfg: Config, args) -> int:
    if not args.yes:
        print("This replaces the local index with a full walk of the Drime folder tree.\n"
              "Stop the gateway first, then re-run with --yes.")
        return 2
    store = _build(cfg)
    t0 = time.monotonic()
    stats = store.rebuild_index()
    print(f"rebuilt index in {time.monotonic() - t0:.0f}s: {stats['objects']} objects, "
          f"{stats['bytes'] / 1024 ** 3:.2f} GiB in {stats['folders']} folders, "
          f"{stats['duplicates']} duplicate entries")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="drime-s3gw", description=__doc__.split("\n\n")[0])
    ap.add_argument("-c", "--config", default=os.environ.get("DRIME_S3GW_CONFIG", "/etc/drime-s3gw/config.toml"))
    ap.add_argument("--version", action="version", version=__version__)
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("serve", help="run the S3 gateway (default)")
    sub.add_parser("check", help="test the Drime token and show usage")
    sub.add_parser("selftest", help="round-trip test objects through Drime")
    rb = sub.add_parser("rebuild-index", help="recreate the local index from Drime")
    rb.add_argument("--yes", action="store_true")
    args = ap.parse_args(argv)
    cfg = Config.load(args.config)
    logging.basicConfig(level=getattr(logging, cfg.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")
    cmd = {"serve": cmd_serve, "check": cmd_check, "selftest": cmd_selftest,
           "rebuild-index": cmd_rebuild}[args.cmd or "serve"]
    return cmd(cfg, args)


if __name__ == "__main__":
    sys.exit(main())
