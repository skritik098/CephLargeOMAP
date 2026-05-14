# Reproducing `LARGE_OMAP_OBJECTS` Warnings on Ceph RGW

This document walks through five scenarios that reliably reproduce the
`LARGE_OMAP_OBJECTS` health warning on a Ceph cluster running RGW, plus a
tuning option that makes every scenario fast enough to use in a lab.

Four scenarios target the **bucket index pool** (`*.rgw.buckets.index`) and one
targets the **meta pool** (`*.rgw.meta`).

A companion Python script ([`large_omap_repro.py`](large_omap_repro.py)) automates all scenarios with parallel execution for faster testing.

---

## Prerequisites

Before running any scenario, make sure you have:

1. A Ceph cluster (any recent release: Pacific, Quincy, Reef, Squid).
2. At least one RGW daemon running and reachable over HTTP/HTTPS.
3. Shell access on a Ceph admin node (for `ceph` / `radosgw-admin` / `rados`).
4. An S3 user with `access_key` and `secret_key`. Create one with:
   ```
   radosgw-admin user create --uid=omaptest --display-name="OMAP Test User"
   ```
5. An S3 client. The companion Python script uses `boto3`; you can also use
   `s3cmd` or `awscli` for manual verification.
6. Pool names. Identify them with:
   ```
   ceph osd pool ls | grep rgw
   ```
   Typical names are `default.rgw.buckets.index` and `default.rgw.meta`. If you
   are on a non-default realm/zone, substitute accordingly throughout.

---

## How to Verify the Warning Appeared

The warning is raised by the **deep scrub** code path — populating OMAP keys
does not trigger it on its own. After running a scenario:

1. Find the PGs that host the relevant pool:
   ```
   ceph pg ls-by-pool default.rgw.buckets.index
   ceph pg ls-by-pool default.rgw.meta
   ```

2. Force a deep scrub on each PG (or on all of them):
   ```
   ceph pg deep-scrub <pgid>
   ```
   Or, to nudge every PG in a pool:
   ```
   for pg in $(ceph pg ls-by-pool default.rgw.buckets.index -f json \
       | jq -r '.pg_stats[].pgid'); do
     ceph pg deep-scrub "$pg"
   done
   ```

3. Wait for scrubs to finish, then check:
   ```
   ceph health detail
   ```
   You should see something like:
   ```
   HEALTH_WARN 1 large omap objects
   [WRN] LARGE_OMAP_OBJECTS: 1 large omap objects
       1 large objects found in pool 'default.rgw.buckets.index'
       Search the cluster log for 'Large omap object found' for more details.
   ```

4. To find the specific RADOS object and key count, search the cluster log:
   ```
   grep -i "Large omap object" /var/log/ceph/<fsid>/ceph.log
   ```
   Or directly inspect a suspected index object:
   ```
   rados -p default.rgw.buckets.index listomapkeys <object-name> | wc -l
   ```

---

## Tuning Option: Lower the Threshold

The default threshold is **200,000 OMAP keys per object**. In a lab you
probably do not want to upload 200k+ objects just to validate the alert path,
so lower the threshold temporarily:

```
ceph config set osd osd_deep_scrub_large_omap_object_key_threshold 5000
```

Optional — lower the value-size threshold too (default 2 GiB):
```
ceph config set osd osd_deep_scrub_large_omap_object_value_sum_threshold 1048576
```

Verify:
```
ceph config get osd osd_deep_scrub_large_omap_object_key_threshold
```

**Restore the defaults after testing:**
```
ceph config rm osd osd_deep_scrub_large_omap_object_key_threshold
ceph config rm osd osd_deep_scrub_large_omap_object_value_sum_threshold
```

Every scenario below assumes you have applied this tuning. If you want to hit
the real 200k threshold instead, just scale the object counts up accordingly.

---

## Scenario 1: Single-Shard Bucket Saturation (Index Pool)

**What it exercises:** the most common cause of bucket-index OMAP bloat —
a bucket whose shard count is too low for the number of objects it holds. This
is the textbook "why we reshard" reproducer.

### Steps

1. **Disable dynamic resharding** so RGW doesn't fix the problem for you:
   ```
   ceph config set client.rgw rgw_dynamic_resharding false
   ```
   Restart the RGW daemons (`systemctl restart ceph-radosgw@*` or via
   `cephadm`/orchestrator) so the change takes effect.

2. **Create the bucket from an S3 client:**
   ```
   aws s3 mb s3://omap-scenario-1 --endpoint-url http://<rgw-host>:<port>
   ```
   or with `s3cmd`:
   ```
   s3cmd --host=<rgw-host>:<port> --no-ssl mb s3://omap-scenario-1
   ```
 
3. **Reshard the bucket down to 1 shard.** Newly created buckets use the
   default shard count (commonly 11 on recent releases), so concentrate every
   key into a single OMAP object:
   ```
   radosgw-admin bucket reshard --bucket=omap-scenario-1 \
       --num-shards=1 --yes-i-really-mean-it
   ```

   Confirm the new shard count:
   ```
   radosgw-admin bucket stats --bucket=omap-scenario-1 | grep num_shards
   ```
 
4. **Upload more objects than the threshold.** With the tuned threshold of
   5000, upload ~6000 zero-byte objects. With the default threshold, upload
   210,000+. Object content is irrelevant — only the key count matters.

5. **Verify the index object grew:**
   ```
   radosgw-admin bucket stats --bucket=omap-scenario-1 | grep -E "num_objects"

   # then for each shard (just shard 0 here):
   rados -p default.rgw.buckets.index listomapkeys .dir.<marker>.0 | wc -l
   ```
 
6. **Force a deep scrub** on the index pool PGs and check `ceph health detail`
   as described in the verification section.

### Cleanup

```
radosgw-admin bucket rm --bucket=omap-scenario-1 --purge-objects
ceph config rm client.rgw rgw_dynamic_resharding
```

---

## Scenario 3: Incomplete Multipart Uploads (Index Pool)

**What it exercises:** orphan upload state. Every initiated-but-not-completed
multipart upload leaves a `meta` entry (and any uploaded parts leave part
placeholders) in the bucket index. These do not appear in `s3 ls` output, so
they are easy to miss until OMAP balloons.

### Steps

1. Make sure dynamic resharding is **off** (so RGW doesn't move the goalposts
   while you're working):
   ```
   ceph config set client.rgw rgw_dynamic_resharding false
   ```
   Restart RGW.

2. **Create the bucket from an S3 client:**
   ```
   aws s3 mb s3://omap-scenario-3 --endpoint-url http://<rgw-host>:<port>
   ```
   or with `s3cmd`:
   ```
   s3cmd --host=<rgw-host>:<port> --no-ssl mb s3://omap-scenario-3
   ```
 

3. **Initiate multipart uploads without completing them.** For each upload,
   call `CreateMultipartUpload` and stop — do not call `CompleteMultipartUpload`
   or `AbortMultipartUpload`. Aim for slightly more than your tuned threshold
   (e.g. 6000 if you set the threshold to 5000).

   With the AWS CLI you'd loop:
   ```
   for i in $(seq 1 6000); do
     aws s3api create-multipart-upload \
         --bucket omap-scenario-3 \
         --key "incomplete-$i" \
         --endpoint-url http://<rgw-host>:<port> >/dev/null
   done
   ```
   The companion Python script does this in parallel.

4. **Confirm the uploads are stuck:**
   ```
   radosgw-admin bucket stats --bucket=omap-scenario-3

   aws s3api list-multipart-uploads \
       --bucket omap-scenario-3 \
       --endpoint-url http://<rgw-host>:<port> | head
   ```

5. Force a deep scrub and check `ceph health detail`.

### Cleanup

```
# Abort all in-flight multipart uploads
radosgw-admin bucket rm --bucket=omap-scenario-3 --purge-objects
ceph config rm client.rgw rgw_dynamic_resharding
```

---

## Scenario 4: Versioned Bucket with Repeated Overwrites (Index Pool)

**What it exercises:** version sprawl. In a versioned bucket every PUT against
an existing key creates a new index entry (the previous version stays); a
relatively small set of keys can therefore generate a very large index.

### Steps

1. Dynamic resharding **off**, restart RGW.

2. **Create a bucket and enable versioning:**
   ```
   aws s3 mb s3://omap-scenario-4 --endpoint-url http://<rgw-host>:<port>
   aws s3api put-bucket-versioning \
       --bucket omap-scenario-4 \
       --versioning-configuration Status=Enabled \
       --endpoint-url http://<rgw-host>:<port>
   ```

3. **Repeatedly overwrite a small set of keys.** For example, 60 keys × 100
   PUTs per key = 6000 index entries. Object body can be empty.

4. **Confirm versions exist:**
   ```
   aws s3api list-object-versions \
       --bucket omap-scenario-4 \
       --max-items 5 \
       --endpoint-url http://<rgw-host>:<port>
   ```

5. Force a deep scrub and check `ceph health detail`.

### Cleanup

Versioned buckets need every version deleted before bucket removal. Easiest:
```
radosgw-admin bucket rm --bucket=omap-scenario-4 --purge-objects --bypass-gc
```

---

## Scenario 5: Delete Marker Accumulation (Index Pool)

**What it exercises:** delete-marker bloat. In a versioned bucket, deleting an
object does not remove it — it writes a delete marker, which is itself a full
index entry. Real-world workloads that PUT + DELETE in a versioned bucket
without lifecycle cleanup hit this often.

### Steps

1. Dynamic resharding **off**, restart RGW.

2. Create a versioned bucket exactly as in scenario 4 (`omap-scenario-5`).

3. **PUT then DELETE many objects.** For example, PUT 6000 unique keys, then
   issue a DELETE (without specifying a version ID) for each — this writes a
   delete marker per key. You end up with ~12,000 index entries from ~6,000
   logical objects.

4. **Confirm delete markers:**
   ```
   aws s3api list-object-versions \
       --bucket omap-scenario-5 \
       --max-items 5 \
       --query 'DeleteMarkers[*].Key' \
       --endpoint-url http://<rgw-host>:<port>
   ```

5. Force a deep scrub and check `ceph health detail`.

### Cleanup

```
radosgw-admin bucket rm --bucket=omap-scenario-5 --purge-objects --bypass-gc
```

---

## Scenario 7: Many Buckets Per User (Meta Pool)

**What it exercises:** meta-pool OMAP growth driven by bucket count. Each
bucket creates a "bucket entry point" object in the meta pool's root
namespace; with enough buckets the tracking objects exceed the threshold.

This is the scenario that puts the warning on `*.rgw.meta` instead of the
index pool, matching the article's "Metadata Pool" section.

### Steps

1. Ensure the user from prerequisites exists (`omaptest`).

2. **Raise the per-user bucket cap** (default is 1000):
   ```
   radosgw-admin user modify --uid=omaptest --max-buckets=100000
   ```

3. **Create many buckets owned by that user.** With the tuned threshold of
   5000, create ~6000 buckets. With the default threshold you'll need
   200,000+, which is impractical without tuning.

   Each bucket name must be globally unique and valid (lowercase, 3–63 chars,
   no underscores). The Python script handles naming.

4. **Confirm the user owns them:**
   ```
   radosgw-admin user info --uid=omaptest | grep num_buckets
   radosgw-admin bucket list --uid=omaptest | wc -l
   ```

5. **Force a deep scrub on the meta pool** specifically:
   ```
   for pg in $(ceph pg ls-by-pool default.rgw.meta -f json \
       | jq -r '.pg_stats[].pgid'); do
     ceph pg deep-scrub "$pg"
   done
   ```

6. Check `ceph health detail`. The warning should reference
   `default.rgw.meta` (not the index pool).

### Cleanup

```
# Bulk-delete all empty buckets the user owns
for b in $(radosgw-admin bucket list --uid=omaptest); do
  radosgw-admin bucket rm --bucket="$b"
done
radosgw-admin user modify --uid=omaptest --max-buckets=1000
```

---

## Final Cleanup Checklist

After you are done testing, restore the cluster to its normal state:

1. **Reset the OMAP thresholds:**
   ```
   ceph config rm osd osd_deep_scrub_large_omap_object_key_threshold
   ceph config rm osd osd_deep_scrub_large_omap_object_value_sum_threshold
   ```

2. **Re-enable dynamic resharding:**
   ```
   ceph config rm client.rgw rgw_dynamic_resharding
   ```
   Restart RGW.

3. **Remove the test user** (only after their buckets are gone):
   ```
   radosgw-admin user rm --uid=omaptest --purge-data
   ```

4. **Confirm health:**
   ```
   ceph health detail
   ```
   Expect `HEALTH_OK` once the next deep-scrub cycle completes.

---

## Using the Python Script

The companion script [`large_omap_repro.py`](large_omap_repro.py) automates all scenarios with parallel execution:

```bash
# Install dependencies
pip install boto3

# 1. Lower the threshold (requires ceph admin access)
sudo ./large_omap_repro.py tune-threshold --keys 5000

# 2. Run a scenario (example: scenario-1)
./large_omap_repro.py scenario-1 \
    --endpoint http://rgw.example.com:8080 \
    --access-key XXXX --secret-key YYYY \
    --count 6000 \
    --disable-resharding \
    --force-single-shard

# 3. Verify the warning
./large_omap_repro.py verify

# 4. Cleanup when done
./large_omap_repro.py cleanup \
    --endpoint http://rgw.example.com:8080 \
    --access-key XXXX --secret-key YYYY
```

**Script Features:**
- Parallel execution with configurable worker count (`--parallelism`)
- Automatic bucket naming with unique suffixes
- Support for environment variables (`RGW_ENDPOINT`, `RGW_ACCESS_KEY`, `RGW_SECRET_KEY`)
- Built-in cleanup for all created resources
- Progress tracking and error reporting

**Available Scenarios:**
- `tune-threshold` - Lower OSD thresholds for faster testing
- `scenario-1` - Single-shard bucket saturation
- `scenario-3` - Incomplete multipart uploads
- `scenario-4` - Versioned bucket with repeated overwrites
- `scenario-5` - Delete marker accumulation
- `scenario-7` - Many buckets per user (meta pool)
- `verify` - Print verification commands
- `cleanup` - Remove all created buckets

Run `./large_omap_repro.py <scenario> --help` for scenario-specific options.

---

## Notes and Caveats

- **Never run this on a production cluster.** Even with cleanup, forcing deep scrubs and bulk uploads consumes significant I/O and RocksDB resources.
- The warning appears only after deep scrub of the affected PG(s), not immediately after uploading data. Always force a deep scrub when validating.
- If dynamic resharding was previously enabled, RGW may have already resharded buckets in the background. Disabling resharding after the fact does not undo completed reshards.
- On multisite setups, run scenarios 1–5 on a single zone only to avoid multiplying cleanup work across zones.
- Versioned-bucket cleanup (scenarios 4 and 5) is slow. Budget adequate time for the `--purge-objects` operation.
- The script uses the bucket name prefix `omap-repro-` for all created buckets, making them easy to identify and clean up.
