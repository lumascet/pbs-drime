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
recommends 64–128 GiB.

**By default PBS writes every chunk it uploads into the cache as well, so the SSD gets a full copy of
every backup's data written to it.** To avoid that, keep only the cache's `.chunks` directory in RAM
(tmpfs) and limit `max-cache-size`. Index files and manifests stay on disk; they are small, roughly
8 MB per TB of backups:

```bash
zfs create -o quota=8G -o atime=on -o relatime=on -o mountpoint=/mnt/pbs-drime-cache rpool/pbs-drime-cache
install -m 755 contrib/pbs-chunk-cache-tmpfs /usr/local/sbin/
install -m 644 'contrib/pbs-chunk-cache-tmpfs@.service' /etc/systemd/system/
systemctl daemon-reload
# after `datastore create` below (it creates .chunks):
systemctl stop proxmox-backup-proxy proxmox-backup
systemctl enable --now "pbs-chunk-cache-tmpfs@$(systemd-escape -p /mnt/pbs-drime-cache).service"
systemctl start proxmox-backup proxmox-backup-proxy
```

The unit mounts a 1 GiB tmpfs on `.chunks` and creates PBS's 65,536 prefix directories before PBS
starts. With `max-cache-size=256MiB`, at most 16 chunks are cached, and markers for the rest are
empty files.

**Measured:** a 1 GiB backup wrote **228 KiB** to the SSD, compared with about 1 GiB with a disk cache.

**After a reboot:** the tmpfs starts empty. Nothing breaks:
- The next GC re-marks every chunk that a backup still references.
- Re-uploads of chunks that already exist are answered from the gateway's index with `412`, without
  a call to Drime.

**If the unit fails,** PBS still starts and caches on disk again.

(`atime` must work, because PBS garbage collection checks it on the cache; tmpfs supports it.)

### Endpoint and datastore

```bash
proxmox-backup-manager s3 endpoint create drime \
  --endpoint 127.0.0.1 --port 8443 --path-style true --region us-west-1 \
  --access-key '<access_key>' --secret-key '<secret_key>' \
  --fingerprint '<AA:BB:...:FF from the openssl command above>' \
  --limit-active-requests 40 --limit-passive-requests 40

proxmox-backup-manager datastore create drime /mnt/pbs-drime-cache \
  --backend type=s3,client=drime,bucket=pbs,max-cache-size=256MiB
proxmox-backup-manager datastore update drime \
  --tuning default-verification-readers=32,default-verification-workers=4
drime-s3gw prefill drime     # create the 4096 chunk folders on Drime before the first backup
```

- `region` and `bucket` must match `config.toml`.
- Use path-style addressing; virtual-host style is not supported.
- Keep `max-cache-size` well below the tmpfs size. Without the tmpfs, set it a little below the
  dataset quota.
- **Verify readers:** 32 parallel readers is the maximum PBS allows. One measured verify read from
  Drime at about 100 MiB/s with 32 readers.
- The request limits are a safety net. The gateway enforces Drime's API budget of about 2,000
  requests per minute itself (`requests_per_minute`), so set the PBS limits high enough that PBS
  keeps enough uploads in flight.

Then add the datastore to PVE as usual (Datacenter → Storage → Add → Proxmox Backup Server), and
schedule **verify**, **prune** and **GC** jobs on it.

## How it maps to Drime

| S3 key (written by PBS) | Drime path under `drime_root` |
|---|---|
| `<store>/.chunks/abcd/<digest>` | `<store>/.chunks/abc/<digest>` |
| `<store>/.cnt/vm/100/2026-…Z/index.json.blob` | same path |
| zero-byte object `…/.namespace` | `…/.namespace.s3gw-empty` (1 byte) |

- **Chunk folders:** a Drime folder can't list more than about 20,000 entries, and creating a folder
  costs an API call. Chunks therefore go into 4,096 folders named by the first three hex digits,
  instead of PBS's 65,536. Folder creation stops after the first few thousand chunks, and each
  folder stays far below the cap (about 80 million chunks in total). Folders are created in
  parallel, and each parent folder is listed only once. Objects written by 0.1.0 in the old
  `.chunks/ab/cd/` layout stay readable and `rebuild-index` still understands them.
- **Index:** the gateway keeps a local SQLite index from each key to its Drime entry, with size,
  MD5/ETag and time. Listings, HEAD requests and `If-None-Match` checks never hit Drime. Only real
  uploads, downloads and deletes do.
- **Integrity:**
  - **Uploads:** PBS sends `Content-MD5` and a SigV4 payload hash, and the gateway checks both.
  - **Downloads:** the gateway checks size and MD5 against the index and retries before anything
    reaches PBS. A silently truncated transfer is retried, not reported to PBS as a corrupt chunk.
  - **Verify:** on top of that, PBS checks every chunk's SHA-256 itself.
- **Drime loses data:**
  - **What happened:** in testing, Drime acknowledged about 1% of uploads during one backup, but
    their data never reached its storage. The file entry existed, and storage answered 404.
  - **Read-after-write check:** the gateway therefore reads one byte back after every upload
    (`verify_uploads`, about 0.2 s and 1 API call) and re-uploads if storage has nothing.
  - **Repair on read:** if an object is later found missing, during a PBS verify or a
    `drime-s3gw scrub`, the gateway forgets it. The next backup that contains that chunk uploads it
    again instead of getting `412`.
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
drime-s3gw [-c CONFIG] prefill STORE      # create a datastore's 4096 chunk folders up front
drime-s3gw [-c CONFIG] scrub [--prefix P]  # check every object's data exists in Drime storage
```

`scrub` makes one cheap ranged request per object; it checked about 800 objects in 25 s. It exits
with status 1 if it finds lost objects, so it can run from a timer and alert you.

The gateway also starts the prefill on its own in the background when a datastore's first chunk
arrives (`prefill_chunk_folders = true`). Until it finishes, an upload into a missing folder waits
for one folder-creation call. Running `prefill <datastore>` before the first backup avoids that
wait entirely.

## Limitations

- **One bucket per gateway.** Several PBS datastores can share it, because PBS prefixes every key with
  the datastore name.
- **Supported S3 operations:**
  - Buckets: ListBuckets, HeadBucket, GetBucketLocation, ListObjects (v1/v2, prefix, delimiter,
    pagination), DeleteObjects.
  - Objects: Put/Get (with Range)/Head/Delete/CopyObject, and conditional `If-None-Match: *`.
- **Not supported:** client multipart uploads, presigned URLs, versioning, ACLs, tagging and
  `aws-chunked` uploads.
- **Throughput:** each chunk upload is one `POST /uploads`, which takes 0.7–5 s depending on load,
  so throughput comes from parallelism (`max_inflight`, default 64).
  - **Measured on a 250 Mbit/s uplink:** real PBS backups reached 26–30 MiB/s, about 220–250 Mbit/s,
    with 64 uploads in flight. That is essentially the full line.
  - **Verify:** with 32 readers, verify read at about 100 MiB/s.
  - **Drime has slow periods:** at times its download redirect took 5–10 s instead of about 0.1 s,
    and folder creation dropped to about 1 per second.

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
