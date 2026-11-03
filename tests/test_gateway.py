"""End-to-end tests: S3 clients -> drime-s3gw (TLS) -> mock Drime.

Two independent S3 clients are used:
- PBSClient: a line-by-line port of proxmox-s3-client's request signing/encoding
  (prepare() + aws_sign_v4_signature() + build_uri()), so we test what PBS sends.
- boto3 (if installed): an independent SigV4 implementation.

Run:  python -m unittest discover -s tests -v
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import http.client
import os
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import drime_s3gw as gw  # noqa: E402
import mock_drime  # noqa: E402

AK, SK, REGION, BUCKET, STORE = "pbs-access", "pbs-secret-key", "us-west-1", "pbs", "drime"
MiB = 1024 * 1024


def aws_encode(s: str, is_key: bool) -> str:
    out = []
    for ch in s:
        if ch.isascii() and (ch.isalnum() or ch in "-._~"):
            out.append(ch)
        elif ch == " ":
            out.append("%20")
        elif ch == "/":
            out.append("/" if is_key else "%2F")
        else:
            out.extend(f"%{b:02X}" for b in ch.encode())
    return "".join(out)


class PBSClient:
    """Mimics proxmox-s3-client 1.4.x (path-style)."""

    def __init__(self, port, secret=SK):
        self.port, self.secret = port, secret
        self.ctx = ssl._create_unverified_context()
        self.conn = None

    def request(self, method, key=None, query=(), body=b"", headers=None, prefix=True, md5=None):
        """key=None: bucket itself; key='': bucket with trailing slash (list/delete);
        otherwise the object key, prefixed with the datastore name like PBS does."""
        if key is None:
            path = "/" + BUCKET
        elif key == "":
            path = "/" + BUCKET + "/"
        else:
            path = "/" + BUCKET + "/" + aws_encode(f"{STORE}/{key}" if prefix else key, True)
        qs = "&".join(aws_encode(k, False) + (("=" + aws_encode(v, False)) if v else "") for k, v in query)
        hdrs = {k.lower(): v for k, v in (headers or {}).items()}
        now = int(time.time())
        datetime_s = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now))
        hdrs["x-amz-date"] = datetime_s
        hdrs["host"] = f"127.0.0.1:{self.port}"
        digest = hashlib.sha256(body).hexdigest()
        hdrs["x-amz-content-sha256"] = digest
        if method in ("PUT", "POST") or (method == "DELETE" and body):
            hdrs["content-length"] = str(len(body))
        if body:
            hdrs["content-md5"] = md5 or base64.b64encode(hashlib.md5(body).digest()).decode()
        canon_headers = sorted(f"{k}:{v}" for k, v in hdrs.items())
        signed = ";".join(sorted(hdrs))
        cq = sorted(f"{aws_encode(k, False)}={aws_encode(v, False)}"
                    for k, v in urllib.parse.parse_qsl(qs, keep_blank_values=True))
        canonical = f"{method}\n{path}\n{'&'.join(cq)}\n{chr(10).join(canon_headers)}\n\n{signed}\n{digest}"
        date = datetime_s[:8]
        scope = f"{date}/{REGION}/s3/aws4_request"
        sts = f"AWS4-HMAC-SHA256\n{datetime_s}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
        k = ("AWS4" + self.secret).encode()
        for part in (date, REGION, "s3", "aws4_request"):
            k = hmac.new(k, part.encode(), hashlib.sha256).digest()
        sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        hdrs["authorization"] = f"AWS4-HMAC-SHA256 Credential={AK}/{scope},SignedHeaders={signed},Signature={sig}"
        if self.conn is None:
            self.conn = http.client.HTTPSConnection("127.0.0.1", self.port, context=self.ctx, timeout=60)
        self.conn.request(method, path + ("?" + qs if qs else ""), body=body or None, headers=hdrs)
        r = self.conn.getresponse()
        data = r.read()
        return r.status, {k.lower(): v for k, v in r.getheaders()}, data


def make_cert(d):
    cert, key = os.path.join(d, "cert.pem"), os.path.join(d, "key.pem")
    openssl = shutil.which("openssl")
    if not openssl:
        raise unittest.SkipTest("openssl not found")
    subprocess.run([openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=127.0.0.1", "-keyout", key, "-out", cert],
                   check=True, capture_output=True)
    return cert, key


class GatewayTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.msrv, cls.mstate = mock_drime.start()
        cert, key = make_cert(cls.tmp)
        cls.cfg = gw.Config(access_key=AK, secret_key=SK, drime_token=mock_drime.TOKEN, bucket=BUCKET,
                            region=REGION, listen="127.0.0.1", port=0, tls=True, cert=cert, key=key,
                            drime_api=f"http://127.0.0.1:{cls.msrv.server_address[1]}/api/v1",
                            drime_root="backups/pbs-s3", index_db=os.path.join(cls.tmp, "index.sqlite"),
                            requests_per_minute=10 ** 6, retries=8, log_level="WARNING")
        gw.Drime.backoff = lambda self, attempt, retry_after=None: time.sleep(0.001)
        cls.store = gw.Store(cls.cfg, gw.Drime(cls.cfg), gw.Index(cls.cfg.index_db))
        cls.srv = gw.make_server(cls.cfg, cls.store)
        cls.port = cls.srv.server_address[1]
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        cls.pbs = PBSClient(cls.port)

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()
        cls.msrv.shutdown()

    def setUp(self):
        self.mstate.fail_rate = 0.0

    # ------------------------------------------------------------------ PBS wire behaviour
    def test_pbs_bucket_and_connection_test(self):
        st, _, _ = self.pbs.request("HEAD")
        self.assertEqual(st, 200)
        # PBS connection test: put/get/delete .s3-client-test (no store prefix)
        ct = {"content-type": "application/octet-stream"}
        st, h, body = self.pbs.request("PUT", ".s3-client-test", body=b"hello", headers=ct, prefix=False)
        self.assertEqual((st, body), (200, b""))
        self.assertTrue(h["etag"].startswith('"'))
        st, _, got = self.pbs.request("GET", ".s3-client-test", prefix=False)
        self.assertEqual((st, got), (200, b"hello"))
        st, _, _ = self.pbs.request("DELETE", ".s3-client-test", prefix=False)
        self.assertEqual(st, 204)
        st, _, xml = self.pbs.request("GET", None, query=[("location", "")])
        self.assertEqual(st, 200)

    def test_pbs_chunk_lifecycle(self):
        data = os.urandom(4 * MiB)  # typical fixed-size VM chunk
        digest = hashlib.sha256(data).hexdigest()
        key = f".chunks/{digest[:4]}/{digest}"
        hdr = {"content-type": "application/octet-stream", "if-none-match": "*"}
        st, h, body = self.pbs.request("PUT", key, body=data, headers=hdr)
        self.assertEqual((st, body), (200, b""))
        self.assertEqual(h["etag"], f'"{hashlib.md5(data).hexdigest()}"')
        st, _, _ = self.pbs.request("PUT", key, body=data, headers=hdr)
        self.assertEqual(st, 412, "existing chunk with If-None-Match must be 412")

        st, h, got = self.pbs.request("GET", key)
        self.assertEqual(st, 200)
        self.assertEqual(got, data)
        for name in ("content-length", "content-type", "etag", "last-modified"):
            self.assertIn(name, h)
        self.assertEqual(len(h["last-modified"]), 29)

        # Drime layout uses 2-level hex folders
        folders, name = gw.key_to_location(f"{STORE}/{key}")
        self.assertEqual(folders, (STORE, ".chunks", digest[:2], digest[2:4]))

        st, _, xml = self.pbs.request("GET", "", query=[("list-type", "2"), ("prefix", f"{STORE}/.chunks/")])
        self.assertEqual(st, 200)
        self.assertIn(f"<Key>{STORE}/{key}</Key>".encode(), xml)
        self.assertIn(b"<IsTruncated>false</IsTruncated>", xml)

        # verify-style "rename" of a bad chunk: CopyObject + DeleteObject
        src = aws_encode(f"{BUCKET}/{STORE}/{key}", True)
        st, _, xml = self.pbs.request("PUT", key + ".0.bad", headers={
            "x-amz-copy-source": src, "content-type": "application/octet-stream",
            "x-amz-metadata-directive": "REPLACE"})
        self.assertEqual(st, 200, xml)
        self.assertIn(b"<CopyObjectResult", xml)
        st, _, body = self.pbs.request("DELETE", key)
        self.assertEqual((st, body), (204, b""))
        st, _, _ = self.pbs.request("GET", key)
        self.assertEqual(st, 404)
        st, _, got = self.pbs.request("GET", key + ".0.bad")
        self.assertEqual((st, got), (200, data))

    def test_pbs_snapshot_files_and_batch_delete(self):
        snap = "ns/prod/vm/100/2026-11-04T10:00:00Z"
        files = {f"{snap}/index.json.blob": os.urandom(800), f"{snap}/drive-scsi0.img.fidx": os.urandom(6 * MiB),
                 "ns/prod/.namespace": b"", "ns/prod/vm/100/owner": b"root@pam\n"}
        for k, v in files.items():
            st, _, _ = self.pbs.request("PUT", k, body=v, headers={"content-type": "application/octet-stream"})
            self.assertEqual(st, 200, k)
        # overwrite (manifest update) keeps exactly one Drime entry
        st, _, _ = self.pbs.request("PUT", f"{snap}/index.json.blob", body=b"v2",
                                    headers={"content-type": "application/octet-stream"})
        self.assertEqual(st, 200)
        folders, name = gw.key_to_location(f"{STORE}/{snap}/index.json.blob")
        fid = self.store.folder_id(folders)
        names = [e["name"] for e in self.mstate.children(fid)]
        self.assertEqual(names.count("index.json.blob"), 1, names)
        self.assertNotIn("index.json.blob (1)", names)
        for k, v in files.items():
            st, _, got = self.pbs.request("GET", k)
            self.assertEqual(got, b"v2" if k.endswith("index.json.blob") else v, k)

        objs = "".join(f"<Object><Key>{STORE}/{k}</Key></Object>" for k in files)
        body = f'<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Quiet>false</Quiet>{objs}</Delete>'.encode()
        st, _, xml = self.pbs.request("POST", "", query=[("delete", "")], body=body)
        self.assertEqual(st, 200, xml)
        self.assertEqual(xml.count(b"<Deleted>"), len(files))
        self.assertNotIn(b"<Error>", xml)
        for k in files:
            self.assertEqual(self.pbs.request("GET", k)[0], 404)

    def test_bad_signature_rejected(self):
        st, _, xml = PBSClient(self.port, secret="wrong").request("GET", "", query=[("list-type", "2")])
        self.assertEqual(st, 403)
        self.assertIn(b"SignatureDoesNotMatch", xml)

    def test_bad_content_md5_rejected(self):
        wrong = base64.b64encode(hashlib.md5(b"other").digest()).decode()
        st, _, xml = self.pbs.request("PUT", "md5test", body=b"abc", md5=wrong,
                                      headers={"content-type": "application/octet-stream"})
        self.assertEqual(st, 400)
        self.assertIn(b"BadDigest", xml)
        self.assertEqual(self.pbs.request("GET", "md5test")[0], 404)

    # ------------------------------------------------------------------ boto3
    def _boto(self):
        try:
            import boto3
            from botocore.config import Config as BC
        except ImportError:
            self.skipTest("boto3 not installed")
        return boto3.client("s3", endpoint_url=f"https://127.0.0.1:{self.port}", aws_access_key_id=AK,
                            aws_secret_access_key=SK, region_name=REGION, verify=False,
                            config=BC(s3={"addressing_style": "path"}, request_checksum_calculation="when_required",
                                      response_checksum_validation="when_required"))

    def test_boto3_listing_pagination_and_delimiter(self):
        s3 = self._boto()
        prefix = "pag/"
        for i in range(1205):
            self.store.put(f"{prefix}{i:05d}", b"x" * (i % 7))
        keys, token = [], None
        pages = 0
        while True:
            kw = {"Bucket": BUCKET, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            r = s3.list_objects_v2(**kw)
            pages += 1
            keys += [o["Key"] for o in r.get("Contents", [])]
            if not r["IsTruncated"]:
                break
            token = r["NextContinuationToken"]
        self.assertEqual(pages, 2)
        self.assertEqual(keys, [f"{prefix}{i:05d}" for i in range(1205)])
        self.store.put("deli/a/1", b"1")
        self.store.put("deli/a/2", b"2")
        self.store.put("deli/b/1", b"3")
        self.store.put("deli/c", b"4")
        r = s3.list_objects_v2(Bucket=BUCKET, Prefix="deli/", Delimiter="/")
        self.assertEqual([p["Prefix"] for p in r["CommonPrefixes"]], ["deli/a/", "deli/b/"])
        self.assertEqual([o["Key"] for o in r["Contents"]], ["deli/c"])
        r = s3.list_objects_v2(Bucket=BUCKET, Prefix="deli/", Delimiter="/", MaxKeys=1)
        self.assertTrue(r["IsTruncated"])
        r2 = s3.list_objects_v2(Bucket=BUCKET, Prefix="deli/", Delimiter="/", MaxKeys=5,
                                ContinuationToken=r["NextContinuationToken"])
        self.assertEqual([p["Prefix"] for p in r2["CommonPrefixes"]], ["deli/b/"])

    def test_boto3_objects(self):
        s3 = self._boto()
        big = os.urandom(12 * MiB + 17)
        s3.put_object(Bucket=BUCKET, Key="b/big", Body=big)
        self.assertEqual(s3.get_object(Bucket=BUCKET, Key="b/big")["Body"].read(), big)
        r = s3.get_object(Bucket=BUCKET, Key="b/big", Range="bytes=100-199")
        self.assertEqual(r["Body"].read(), big[100:200])
        s3.put_object(Bucket=BUCKET, Key="b/empty", Body=b"")
        self.assertEqual(s3.head_object(Bucket=BUCKET, Key="b/empty")["ContentLength"], 0)
        self.assertEqual(s3.get_object(Bucket=BUCKET, Key="b/empty")["Body"].read(), b"")
        s3.put_object(Bucket=BUCKET, Key="b/x:y z", Body=b"colon")
        self.assertEqual(s3.get_object(Bucket=BUCKET, Key="b/x:y z")["Body"].read(), b"colon")
        with self.assertRaises(Exception):
            s3.put_object(Bucket=BUCKET, Key="b/x:y z", Body=b"again", IfNoneMatch="*")
        s3.delete_object(Bucket=BUCKET, Key="b/big")
        with self.assertRaises(Exception):
            s3.head_object(Bucket=BUCKET, Key="b/big")

    # ------------------------------------------------------------------ resilience
    def test_fault_injection(self):
        self.mstate.fail_rate = 0.25
        payloads = {f"faulty/.chunks/{i:04x}/{hashlib.sha256(str(i).encode()).hexdigest()}": os.urandom(50_000 + i)
                    for i in range(60)}
        payloads["faulty/large"] = os.urandom(11 * MiB)
        for k, v in payloads.items():
            st, _, _ = self.pbs.request("PUT", k, body=v, headers={"content-type": "application/octet-stream",
                                                                    "if-none-match": "*"})
            self.assertIn(st, (200, 503), k)
            if st == 503:  # PBS would retry; so do we
                st, _, _ = self.pbs.request("PUT", k, body=v, headers={"content-type": "application/octet-stream",
                                                                        "if-none-match": "*"})
                self.assertIn(st, (200, 412), k)
        self.mstate.fail_rate = 0.0
        for k, v in payloads.items():
            st, _, got = self.pbs.request("GET", k)
            self.assertEqual((st, got), (200, v), k)
        # every indexed object maps to exactly one live Drime file: no orphans, no duplicates
        live = [e for e in self.mstate.entries.values() if e["type"] == "file" and not e["deleted"]]
        n_index, _ = self.store.index.stats()
        self.assertEqual(len(live), n_index, "orphaned or duplicate Drime files after faults")
        self.assertEqual(self.mstate.auth_leaks, 0)

    def test_concurrent_uploads(self):
        from concurrent.futures import ThreadPoolExecutor
        ct = {"content-type": "application/octet-stream", "if-none-match": "*"}
        same = os.urandom(100_000)
        d = hashlib.sha256(same).hexdigest()
        same_key = f"conc/.chunks/{d[:4]}/{d}"
        distinct = {f"conc/.chunks/{i:02x}{i:02x}/{hashlib.sha256(bytes([i])).hexdigest()}": os.urandom(20_000)
                    for i in range(48)}

        def put(kv):
            return PBSClient(self.port).request("PUT", kv[0], body=kv[1], headers=ct)[0]

        with ThreadPoolExecutor(16) as ex:
            same_results = list(ex.map(put, [(same_key, same)] * 12))
            distinct_results = list(ex.map(put, distinct.items()))
        self.assertEqual(sorted(same_results), [200] + [412] * 11)
        self.assertEqual(set(distinct_results), {200})
        folders, _ = gw.key_to_location(f"{STORE}/{same_key}")
        self.assertEqual(len(self.mstate.children(self.store.folder_id(folders))), 1)
        # racing threads must not create duplicate folders anywhere
        for fid in {e["parent_id"] for e in self.mstate.entries.values() if e["type"] == "folder"}:
            names = [e["name"] for e in self.mstate.children(fid) if e["type"] == "folder"]
            self.assertEqual(len(names), len(set(names)), f"duplicate folders under {fid}: {names}")
        for k, v in distinct.items():
            self.assertEqual(self.pbs.request("GET", k)[2], v)

    def test_zz_rebuild_index(self):
        # runs last (alphabetical): everything uploaded so far must survive an index rebuild
        before = {o.key: o.size for o in self.store.list("", "", 10 ** 9, "")[0]}
        self.assertTrue(before)
        stats = self.store.rebuild_index()
        after = {o.key: o.size for o in self.store.list("", "", 10 ** 9, "")[0]}
        self.assertEqual(after, before)
        self.assertEqual(stats["duplicates"], 0)
        k = next(k for k in before if before[k] > 0)
        self.assertEqual(len(self.store.get(k)[1]), before[k])
        self.assertEqual(self.mstate.auth_leaks, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
