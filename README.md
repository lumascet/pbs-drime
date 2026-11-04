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

> **Status: in use against a real Drime account with PBS 4.2.**
> - **Tested on real Drime:** real CT and VM backups, verify, a full CT restore, and a disaster-recovery
>   rebuild of the index from Drime alone.
> - **Drime has silently lost acknowledged uploads before,** which is why every upload is read back
>   and checked (see below).
> - **Before you rely on it:** run `drime-s3gw selftest` against your own account; see
>   [First run](#first-run).

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
8 MB per TB of backups.

The recommended layout puts the cache directory on a **sparse ext4 image** whose size equals
`max_total_size`. That's what lets the PBS gauge show the Drime usage; see
[Storage used on Drime, and a limit](#storage-used-on-drime-and-a-limit). The image lives in a ZFS
dataset with compression, so it costs almost nothing on disk:

```bash
zfs create -o compression=lz4 -o quota=8G -o mountpoint=/var/lib/pbs-drime rpool/pbs-drime
truncate -s 3T /var/lib/pbs-drime/drime.img            # = max_total_size
mkfs.ext4 -F -m 0 -N 1000000 -I 256 -J size=64 -E lazy_itable_init=1,lazy_journal_init=1 /var/lib/pbs-drime/drime.img
mkdir -p /mnt/datastore/drime
echo '/var/lib/pbs-drime/drime.img /mnt/datastore/drime ext4 loop,nofail,x-systemd.requires-mounts-for=/var/lib/pbs-drime 0 0' >> /etc/fstab
systemctl daemon-reload && mount /mnt/datastore/drime && rmdir /mnt/datastore/drime/lost+found

install -m 755 contrib/pbs-chunk-cache-tmpfs /usr/local/sbin/
install -m 644 'contrib/pbs-chunk-cache-tmpfs@.service' /etc/systemd/system/
systemctl daemon-reload
# after `datastore create` below (it creates .chunks):
systemctl stop proxmox-backup-proxy proxmox-backup
systemctl enable --now "pbs-chunk-cache-tmpfs@$(systemd-escape -p /mnt/datastore/drime).service"
systemctl start proxmox-backup proxmox-backup-proxy
```

If you don't want the gauge, a plain ZFS dataset mounted at `/mnt/datastore/drime` works just as well.

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
  --limit-active-requests 100 --limit-passive-requests 150

proxmox-backup-manager datastore create drime /mnt/datastore/drime \
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
- **Request limits:** these are only a safety net. The gateway enforces Drime's API budget itself
  (`requests_per_minute`; Drime reports `x-ratelimit-limit: 10000` per minute) and slows down when
  Drime reports few requests remaining.
  - Don't set the PBS limits low: with small chunks, `--limit-passive-requests 40` capped verify at
    40 chunks/s, about 35 MiB/s.

Then add the datastore to PVE as usual (Datacenter → Storage → Add → Proxmox Backup Server), and
schedule jobs on it:

```bash
proxmox-backup-manager datastore update drime --gc-schedule 'sun 03:00' --verify-new true
proxmox-backup-manager prune-job create p-drime --store drime --schedule 'mon,wed,sat 21:30' \
  --keep-daily 7 --keep-weekly 1 --keep-monthly 1
proxmox-backup-manager verify-job create v-drime --store drime --schedule 'mon,wed,sat 23:00' \
  --ignore-verified true --outdated-after 30
```

- **`verify-new`:** every backup is read back from Drime right after it finishes. Since Drime has
  lost acknowledged data before, this is the check worth having.
- **Without GC:** deleted snapshots never free space on Drime.

### Storage used on Drime, and a limit

The PBS UI computes a datastore's usage from the local cache directory, so for an S3 datastore it
shows the cache, not Drime. To see the real numbers:

- **The gateway knows them:** run `drime-s3gw usage` for the totals per datastore, the limit and the
  Drime account.
- **Show them in the PBS UI:** the `drime-s3gw-usage@<datastore>` timer runs every 15 minutes. It does
  two things:
  - It writes the numbers into the datastore's **comment**, which is visible in the PBS datastore
    list.
  - With the sparse-image layout above, it also adjusts a reservation file so the **usage gauge**
    shows the Drime usage against the image size, i.e. against your limit.

```bash
install -m 644 'contrib/drime-s3gw-usage@.service' 'contrib/drime-s3gw-usage@.timer' /etc/systemd/system/
mkdir -p '/etc/systemd/system/drime-s3gw-usage@.service.d'
cat > '/etc/systemd/system/drime-s3gw-usage@.service.d/reserve.conf' <<'EOF'
[Service]
ExecStart=
ExecStart=/usr/bin/python3 /usr/local/bin/drime-s3gw -c /etc/drime-s3gw/config.toml usage --pbs-comment %i --reserve-file /mnt/datastore/drime/.drime-usage/reserve
EOF
systemctl daemon-reload && systemctl enable --now drime-s3gw-usage@drime.timer
```

Skip the drop-in if you only want the comment.

- **The limit:** `max_total_size` (e.g. `"1TiB"`) caps what the gateway stores. Once it's reached:
  - New chunks are refused with `403 QuotaExceeded`, so the backup fails with a clear error and your
    PBS notifications report it.
  - Chunks that already exist still deduplicate, metadata still writes, and prune and GC still free
    space.
- **Warning before the limit:** above `usage_warn_percent` (default 90%), the usage unit exits 1 about
  once a day. Hook an `OnFailure=` alert to it.

**How the gauge works:** PBS reads `statfs()` of the cache directory.
- **"Total":** on the sparse image, this is the image size, which equals your limit.
- **"Used":** the usage timer `fallocate`s `.drime-usage/reserve` so that it matches what is stored on
  Drime.

How it stays light on the SSD:
- **No real data:** `fallocate` only allocates blocks; nothing is written.
- **Zero blocks:** with ZFS compression enabled, ext4's zeroed inode tables are stored as holes.
- **Small inode count:** the cache directory only holds metadata (`.chunks` lives on tmpfs), so
  `-N 1000000` is plenty.

The reservation always leaves at least 2 GiB available, so PBS's own cache keeps working.

### Bandwidth limits

Set these in `config.toml`, e.g. to leave room on your line for other traffic:

```toml
upload_limit = "200Mbit"     # or "25MiB" (per second); "" = unlimited
download_limit = "500Mbit"
```

- **What they cover:** all traffic between the gateway and Drime. That includes backups, the
  read-back check after uploads, verify, restores, read-ahead and scrub.
- **How they work:** one shared byte-rate bucket per direction, inside the gateway process.
  Separate commands such as `scrub` or `backup-index` run in their own process and have their own
  bucket.
- **How strictly:** the long-run rate never exceeds the cap; individual transfers can still burst.
- **Alternative:** PBS's own endpoint options (`--rate-in`, `--rate-out`) also work, but they only
  cover PBS's traffic to the gateway, not the gateway's own checks.

### Hardening

```bash
install -m 644 contrib/drime-s3gw-{scrub,index-backup}.{service,timer} /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now drime-s3gw-scrub.timer drime-s3gw-index-backup.timer
```

- **Upload check:** set `verify_uploads = "full"` to read every upload back completely and compare
  its MD5. This also catches corrupted content, not just missing data. It doubles download traffic
  during backups.
- **Alerts:** add `OnFailure=` drop-ins, e.g. a Telegram or mail unit, for `drime-s3gw`,
  `drime-s3gw-scrub`, `drime-s3gw-index-backup` and `pbs-chunk-cache-tmpfs@`. The scrub exits 1
  when it finds lost objects.
- **Encryption key:** back it up off the host, e.g.
  `proxmox-backup-client key paperkey /etc/pve/priv/storage/<storage>.enc --output-format text`
  into your password manager. Without it, the backups on Drime cannot be restored.

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
2. Run `drime-s3gw rebuild-index --yes`. It walks the Drime folder tree, listing each tree level in
   parallel. Measured: about 8,200 folders and 3,700 objects took 164 s, with a result identical to
   the live index.
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
drime-s3gw [-c CONFIG] usage [--pbs-comment DATASTORE]  # storage used on Drime, limit, account
drime-s3gw [-c CONFIG] migrate-layout [--dry-run]       # move 0.1.0-layout chunks to the current layout
drime-s3gw [-c CONFIG] backup-index        # copy the index to Drime (_s3gw/index.sqlite)
drime-s3gw [-c CONFIG] restore-index --yes # fetch that copy back (fast path; see below)
drime-s3gw --index-db FILE <command>       # use another index file, e.g. rebuild into a scratch file
```

**Index backup vs rebuild:**
- **`rebuild-index` is authoritative:** it reconstructs the index from what is actually on Drime.
- **`restore-index` is faster but can be stale:** it doesn't know objects stored after the last
  `backup-index`.
- **Check the rebuild without risk:**
  `drime-s3gw --index-db /run/test.sqlite rebuild-index --yes`, then compare it with the live index.

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
  - **Measured on a 250 Mbit/s uplink:** uploads reach 195–240 Mbit/s with 64 in flight, for 1.3 MiB
    chunks as well as 4 MiB chunks. That is essentially the full line.
    - **The read-back check:** it runs outside the 64 upload slots, so it doesn't reduce the number
      of uploads in progress.
    - **Beyond 64 uploads at once:** Drime gets slower and starts dropping connections.
  - **Verify:** with 32 readers, verify read at about 100 MiB/s (about 800–900 Mbit/s), with small
    chunks too. A real 7 GiB CT verified in 67 s. Read-ahead stays out of verify jobs; it only
    follows single sequential readers.
  - **Restores:** PBS restores fetch chunks strictly one at a time. Without help they run at
    chunk size divided by latency, about 10 MiB/s here. The gateway therefore remembers the chunk
    order of every index file PBS writes (`.didx`/`.fidx`, about 8 MB per TB per snapshot).
    When a chunk is requested, it fetches the next `readahead_window` chunks (default 24) in
    parallel into a RAM cache (`readahead_cache_mib`, default 256).
    - **Measured:** a sequential 1.2 GiB restore read went from 9.9 to **67 MiB/s**, with no
      wasted prefetches.
    - **Safeguard:** read-ahead for a snapshot stops if its prefetched chunks go unused.
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
