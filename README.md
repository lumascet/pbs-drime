# pbs-drime

An S3-compatible gateway that lets **Proxmox Backup Server** use **[Drime](https://drime.cloud)** cloud
storage as a datastore.

- Backups are written **straight to Drime**. No second disk holds a copy of the datastore.
- **Verify jobs run online:** PBS downloads every chunk from Drime and checks its digest.
- Garbage collection, prune, sync, restore and file-level restore work as they do on any PBS S3
  datastore.

```
PVE / PBS ──S3 (HTTPS, SigV4)──▶ drime-s3gw ──Drime REST API──▶ Drime
               127.0.0.1:8443        │
                                     └─ SQLite index (S3 key → Drime entry)
```

Drime has no S3 interface. It has its own REST API, so the gateway translates between the two.
It is a single Python file that uses only the standard library, so nothing has to be installed on the
hypervisor.

> **Status: tested against a mock of the Drime API only.** Run `drime-s3gw selftest` against your
> account before trusting it with backups; see [First run](#first-run).

## Requirements

- Proxmox Backup Server **4.1.9 or newer**. S3 datastores became a supported feature in 4.1.9; 4.2.x is
  recommended.
- Python 3.11+ (PVE 8 and 9 ship with 3.11 and 3.13).
- A Drime account and a personal access token.
- A small local directory for the PBS cache (see [The local cache](#the-local-cache)).

## Install (on the PBS host)

```bash
install -m 755 drime_s3gw.py /usr/local/bin/drime-s3gw
install -d -m 700 /etc/drime-s3gw
install -m 600 config.example.toml /etc/drime-s3gw/config.toml
editor /etc/drime-s3gw/config.toml          # access/secret key, Drime token
```

Generate a self-signed certificate. PBS pins it by fingerprint, so no CA is needed:

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
  -subj "/CN=drime-s3gw" -addext "subjectAltName=IP:127.0.0.1" \
  -keyout /etc/drime-s3gw/key.pem -out /etc/drime-s3gw/cert.pem
chmod 600 /etc/drime-s3gw/key.pem
openssl x509 -in /etc/drime-s3gw/cert.pem -noout -fingerprint -sha256   # for PBS below
```

### First run

```bash
drime-s3gw check      # token valid? quota? creates the root folder
drime-s3gw selftest   # uploads/reads/overwrites/deletes test objects (small, 12 MiB, empty)
```

`selftest` covers the parts of the Drime API that are undocumented: multipart uploads, file names
without an extension, empty files, overwrites and deletes. It cleans up after itself. Don't continue if
it reports `FAILED`.

```bash
install -m 644 drime-s3gw.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now drime-s3gw
journalctl -u drime-s3gw -f
```

## Configure PBS

### The local cache

PBS requires a local cache directory for every S3 datastore. It holds the index files, one marker
file per chunk, and an LRU cache of chunk data. It is **not** a full copy of the datastore. Proxmox
recommends 64–128 GiB. Smaller works too, at the cost of more downloads during restores. A ZFS dataset
with a quota keeps the cache from growing past its limit:

```bash
zfs create -o quota=32G -o atime=on -o relatime=on -o mountpoint=/mnt/pbs-drime-cache rpool/pbs-drime-cache
```

(`atime` must work, because PBS garbage collection checks it on the cache.)

### Endpoint and datastore

```bash
proxmox-backup-manager s3 endpoint create drime \
  --endpoint 127.0.0.1 --port 8443 --path-style true --region us-west-1 \
  --access-key '<access_key>' --secret-key '<secret_key>' \
  --fingerprint '<AA:BB:...:FF from the openssl command above>' \
  --limit-active-requests 15 --limit-passive-requests 10

proxmox-backup-manager datastore create drime /mnt/pbs-drime-cache \
  --backend type=s3,client=drime,bucket=pbs,max-cache-size=28GiB
```

- `region` and `bucket` must match `config.toml`.
- Use path-style addressing; virtual-host style is not supported.
- Set `max-cache-size` a little below the dataset quota.
- The request limits keep PBS under Drime's API budget, which is about 2000 requests per minute per
  account. The gateway also throttles itself (`requests_per_minute`), so the limits mainly keep PBS
  from queueing.

Then add the datastore to PVE as usual (Datacenter → Storage → Add → Proxmox Backup Server), and
schedule **verify**, **prune** and **GC** jobs on it.

## How it maps to Drime

| S3 key (written by PBS) | Drime path under `drime_root` |
|---|---|
| `<store>/.chunks/abcd/<digest>` | `<store>/.chunks/ab/cd/<digest>` |
| `<store>/.cnt/vm/100/2026-…Z/index.json.blob` | same path |
| zero-byte object `…/.namespace` | `…/.namespace.s3gw-empty` (1 byte) |

- **Chunk folders:** a Drime folder can't list more than about 20,000 entries. The chunk store is
  therefore split two levels deep (256 × 256 folders) instead of PBS's 65,536 folders side by side.
- **Index:** the gateway keeps a local SQLite index from each key to its Drime entry, with size,
  MD5/ETag and time. Listings, HEAD requests and `If-None-Match` checks never hit Drime. Only real
  uploads, downloads and deletes do.
- **Integrity:**
  - **Uploads:** PBS sends `Content-MD5` and a SigV4 payload hash, and the gateway checks both.
  - **Downloads:** the gateway checks size and MD5 against the index and retries before anything
    reaches PBS. A silently truncated transfer is retried, not reported to PBS as a corrupt chunk.
  - **Verify:** on top of that, PBS checks every chunk's SHA-256 itself.
- **Failure handling:**
  - Drime errors (429, 5xx) are retried with backoff.
  - When a failed upload might have been stored anyway, the gateway looks for it on Drime and checks
    its content before adopting it, so retries don't leave duplicates.
  - If Drime is still down after the retries, PBS gets a `503` and retries itself.

## Security notes

- **Drime can see your data unless PBS encrypts it:** use client-side encryption (an encryption key on
  the PVE storage). Even then, metadata such as VM/CT IDs, snapshot times, notes and owners is
  readable.
- **Keep it local:** the gateway listens on `127.0.0.1` by default. If PBS runs on another machine, bind
  to a private address and firewall it.
- **Protect the config:** `config.toml` contains your Drime token, which has full access to your Drime
  account. Keep it `0600`.

## Disaster recovery

The data on Drime is self-contained. To bring it back on a new host:

1. Install the gateway with the same `bucket`, `drime_root` and Drime token.
2. Run `drime-s3gw rebuild-index --yes`. It walks the Drime folder tree and needs roughly one API call
   per folder, so a large datastore takes about 30–60 minutes.
3. Start the service. Recreate the PBS endpoint, and recreate the datastore **with the same name**,
   adding `--reuse-datastore true --overwrite-in-use true`. PBS then runs an S3 refresh to restore its
   local cache.

The same rebuild fixes a lost or corrupted index on the existing host. You can back up
`/var/lib/drime-s3gw/index.sqlite` to skip the walk, but copy it only while the service is stopped, or
use `sqlite3 index.sqlite ".backup copy.sqlite"`.

## Commands

```
drime-s3gw [-c CONFIG] serve             # default; the gateway
drime-s3gw [-c CONFIG] check             # token, quota, root folder, index stats
drime-s3gw [-c CONFIG] selftest          # round-trip test against your Drime account
drime-s3gw [-c CONFIG] rebuild-index --yes
```

## Limitations

- **One bucket per gateway.** Several PBS datastores can share it, because PBS prefixes every key with
  the datastore name.
- **Supported S3 operations:**
  - Buckets: ListBuckets, HeadBucket, GetBucketLocation, ListObjects (v1/v2, prefix, delimiter,
    pagination), DeleteObjects.
  - Objects: Put/Get (with Range)/Head/Delete/CopyObject, and conditional `If-None-Match: *`.
- **Not supported:** client multipart uploads, presigned URLs, versioning, ACLs, tagging and
  `aws-chunked` uploads.
- **Throughput:** Drime's ~2000 API requests per minute limit throughput to roughly 60–100 MiB/s with
  4 MiB chunks. A verify downloads every chunk a snapshot references.

## Development

```bash
python -m pip install boto3      # optional, enables the boto3 tests
python -m unittest discover -s tests -v
```

The tests run the gateway over TLS against an in-memory mock of the Drime API (`tests/mock_drime.py`)
and use two independent S3 clients:

- a Python port of PBS's own request signing (`proxmox-s3-client`);
- boto3.

They cover the PBS chunk, snapshot, GC and verify request patterns, concurrent uploads and listing
pagination. They also inject faults: 429s, 5xx errors, and 5xx errors after the upload was already
stored. Finally they rebuild the index and compare it with the original.
