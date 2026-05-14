#!/usr/bin/env python3
"""
large_omap_repro.py
===================

Reproduce LARGE_OMAP_OBJECTS warnings on a Ceph cluster running RGW.

Each scenario is selectable via a subcommand so you can run them independently
in any order. See `--help` for the full list.

Scenarios:
    tune-threshold  Lower the OSD large-omap key threshold (makes testing fast).
    scenario-1      Single-shard bucket saturated with N objects (index pool).
    scenario-3      N initiated-but-not-completed multipart uploads (index pool).
    scenario-4      Versioned bucket with many overwrites of a small key set
                    (index pool).
    scenario-5      Versioned bucket with N PUT+DELETE pairs producing delete
                    markers (index pool).
    scenario-7      One user owning N buckets (meta pool).
    verify          Run cluster-side checks for the large omap warning.
    cleanup         Best-effort cleanup of buckets/users this script created.

Quick start:
    # 1. Make the threshold reachable in a lab
    sudo ./large_omap_repro.py tune-threshold --keys 5000

    # 2. Pick a scenario (uses the tuned threshold above)
    ./large_omap_repro.py scenario-1 \
        --endpoint http://rgw.example.com:8080 \
        --access-key XXXX --secret-key YYYY \
        --count 6000 \
        --disable-resharding \
        --force-single-shard

    # 3. Verify (this just prints the commands to run on a Ceph admin node)
    ./large_omap_repro.py verify

    # 4. Cleanup when done
    ./large_omap_repro.py cleanup \
        --endpoint http://rgw.example.com:8080 \
        --access-key XXXX --secret-key YYYY

Requirements:
    pip install boto3
    The 'tune-threshold' and 'verify' subcommands shell out to `ceph` /
    `radosgw-admin` and therefore must run on a Ceph admin node. Scenarios
    1, 3, 4, 5 only need S3 endpoint access. Scenario 7 needs `radosgw-admin`
    to bump the user's max-buckets and is best run on an admin node too.
"""

import argparse
import concurrent.futures
import logging
import os
import random
import shutil
import string
import subprocess
import sys
import time
from typing import List, Optional

try:
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError
except ImportError:
    sys.stderr.write(
        "boto3 is required. Install with: pip install boto3\n"
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("omap-repro")


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_USER = "omaptest"
DEFAULT_DISPLAY_NAME = "OMAP Test User"
DEFAULT_REGION = "us-east-1"
DEFAULT_PARALLELISM = 32
DEFAULT_COUNT = 6000  # tuned for threshold=5000; bump to 210_000 for real test

# Bucket name prefix to make this script's leftovers identifiable.
BUCKET_PREFIX = "omap-repro"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def s3_client(endpoint: str, access_key: str, secret_key: str,
              region: str = DEFAULT_REGION,
              verify_tls: bool = True):
    """Build a boto3 S3 client configured for RGW (path-style addressing)."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
            max_pool_connections=64,
        ),
        verify=verify_tls,
    )


def have_command(name: str) -> bool:
    return shutil.which(name) is not None


def run_cmd(cmd: List[str], check: bool = True,
            capture: bool = False) -> subprocess.CompletedProcess:
    log.info("$ %s", " ".join(cmd))
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        capture_output=capture,
    )


def require_admin_tools(*tools: str) -> None:
    missing = [t for t in tools if not have_command(t)]
    if missing:
        log.error(
            "Missing required admin tool(s): %s. Run this subcommand on a "
            "Ceph admin node.",
            ", ".join(missing),
        )
        sys.exit(2)


def parallel_map(fn, items, parallelism: int, desc: str = "tasks") -> int:
    """Run fn(item) over items concurrently, returning success count."""
    done = 0
    failed = 0
    total = len(items)
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as ex:
        futures = [ex.submit(fn, item) for item in items]
        for i, f in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                f.result()
                done += 1
            except Exception as e:  # noqa: BLE001
                failed += 1
                if failed <= 5:
                    log.warning("Task failed: %s", e)
            if i % max(1, total // 20) == 0 or i == total:
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed > 0 else 0
                log.info(
                    "%s: %d/%d (%.0f/s, %d failed)",
                    desc, i, total, rate, failed,
                )
    if failed:
        log.warning("%d/%d %s failed", failed, total, desc)
    return done


def unique_suffix(n: int = 6) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


# ---------------------------------------------------------------------------
# tune-threshold (Scenario 2)
# ---------------------------------------------------------------------------

def cmd_tune_threshold(args) -> None:
    """Lower the OSD large-omap thresholds so scenarios trigger faster."""
    require_admin_tools("ceph")

    if args.restore:
        log.info("Restoring default OSD large-omap thresholds.")
        run_cmd(["ceph", "config", "rm", "osd",
                 "osd_deep_scrub_large_omap_object_key_threshold"],
                check=False)
        run_cmd(["ceph", "config", "rm", "osd",
                 "osd_deep_scrub_large_omap_object_value_sum_threshold"],
                check=False)
        return

    log.info("Setting osd_deep_scrub_large_omap_object_key_threshold = %d",
             args.keys)
    run_cmd(["ceph", "config", "set", "osd",
             "osd_deep_scrub_large_omap_object_key_threshold", str(args.keys)])

    if args.value_bytes is not None:
        log.info(
            "Setting osd_deep_scrub_large_omap_object_value_sum_threshold = %d",
            args.value_bytes,
        )
        run_cmd(["ceph", "config", "set", "osd",
                 "osd_deep_scrub_large_omap_object_value_sum_threshold",
                 str(args.value_bytes)])

    log.info("Done. Run a scenario, then `verify` to check the warning.")


# ---------------------------------------------------------------------------
# User / bucket bootstrap helpers
# ---------------------------------------------------------------------------

def ensure_user_via_admin(uid: str, display_name: str,
                          max_buckets: Optional[int] = None) -> None:
    """Create/update the test user using radosgw-admin (must be on admin node)."""
    require_admin_tools("radosgw-admin")
    res = subprocess.run(
        ["radosgw-admin", "user", "info", "--uid", uid],
        capture_output=True, text=True,
    )
    if res.returncode != 0:
        log.info("Creating user %s", uid)
        run_cmd(["radosgw-admin", "user", "create",
                 "--uid", uid, "--display-name", display_name])
    else:
        log.info("User %s already exists", uid)

    if max_buckets is not None:
        log.info("Setting max-buckets=%d on user %s", max_buckets, uid)
        run_cmd(["radosgw-admin", "user", "modify",
                 "--uid", uid, "--max-buckets", str(max_buckets)])


def disable_dynamic_resharding() -> None:
    """Best-effort disable of RGW dynamic resharding for index-pool scenarios."""
    if not have_command("ceph"):
        log.warning(
            "ceph CLI not found; SKIPPING dynamic-resharding disable. "
            "Disable manually before running index-pool scenarios."
        )
        return
    log.info("Disabling rgw_dynamic_resharding (you must restart RGW after this).")
    run_cmd(["ceph", "config", "set", "client.rgw",
             "rgw_dynamic_resharding", "false"])
    log.warning(
        "Restart RGW now (e.g. `systemctl restart 'ceph-radosgw@*'` or via "
        "your orchestrator) before continuing."
    )


def create_bucket_single_shard(s3, bucket: str) -> None:
    """Create a bucket. With dynamic resharding off, it stays at the default shard count.
    For a true single-shard bucket on releases that default to 11 shards, use
    `radosgw-admin bucket reshard --num-shards=1` after creation.
    """
    try:
        s3.create_bucket(Bucket=bucket)
        log.info("Created bucket %s", bucket)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            log.info("Bucket %s already exists", bucket)
        else:
            raise


def force_single_shard_via_admin(bucket: str) -> None:
    """Reshard the bucket down to 1 shard (concentrates all keys in one OMAP object)."""
    if not have_command("radosgw-admin"):
        log.warning(
            "radosgw-admin not available; bucket will use the default shard "
            "count. The scenario still works but you may need a higher object "
            "count to cross the threshold."
        )
        return
    log.info("Resharding %s down to 1 shard", bucket)
    run_cmd(["radosgw-admin", "bucket", "reshard",
             "--bucket", bucket, "--num-shards", "1", "--yes-i-really-mean-it"])


# ---------------------------------------------------------------------------
# Scenario 1: single-shard bucket saturation
# ---------------------------------------------------------------------------

def cmd_scenario_1(args) -> None:
    log.info("=== Scenario 1: single-shard bucket saturation (index pool) ===")
    if args.disable_resharding:
        disable_dynamic_resharding()

    s3 = s3_client(args.endpoint, args.access_key, args.secret_key,
                   region=args.region, verify_tls=not args.insecure)
    bucket = args.bucket or f"{BUCKET_PREFIX}-1-{unique_suffix()}"

    create_bucket_single_shard(s3, bucket)
    if args.force_single_shard:
        force_single_shard_via_admin(bucket)

    log.info("Uploading %d zero-byte objects to %s (parallelism=%d)",
             args.count, bucket, args.parallelism)

    def _put(i: int) -> None:
        s3.put_object(Bucket=bucket, Key=f"obj-{i:08d}", Body=b"")

    parallel_map(_put, list(range(args.count)),
                 parallelism=args.parallelism, desc="PUTs")

    log.info("Scenario 1 complete. Bucket: %s", bucket)
    log.info("Next: run `%s verify` and force a deep scrub on the index pool.",
             sys.argv[0])


# ---------------------------------------------------------------------------
# Scenario 3: incomplete multipart uploads
# ---------------------------------------------------------------------------

def cmd_scenario_3(args) -> None:
    log.info("=== Scenario 3: incomplete multipart uploads (index pool) ===")
    if args.disable_resharding:
        disable_dynamic_resharding()

    s3 = s3_client(args.endpoint, args.access_key, args.secret_key,
                   region=args.region, verify_tls=not args.insecure)
    bucket = args.bucket or f"{BUCKET_PREFIX}-3-{unique_suffix()}"

    create_bucket_single_shard(s3, bucket)
    if args.force_single_shard:
        force_single_shard_via_admin(bucket)

    log.info("Initiating %d multipart uploads on %s WITHOUT completing them",
             args.count, bucket)

    def _initiate(i: int) -> None:
        s3.create_multipart_upload(Bucket=bucket, Key=f"incomplete-{i:08d}")

    parallel_map(_initiate, list(range(args.count)),
                 parallelism=args.parallelism, desc="initiated MPUs")

    # Sanity: list a few in-flight MPUs so the user can confirm.
    try:
        resp = s3.list_multipart_uploads(Bucket=bucket, MaxUploads=5)
        n_listed = len(resp.get("Uploads", []))
        log.info("list-multipart-uploads sees at least %d in-flight upload(s).",
                 n_listed)
    except ClientError as e:
        log.warning("list-multipart-uploads failed: %s", e)

    log.info("Scenario 3 complete. Bucket: %s", bucket)


# ---------------------------------------------------------------------------
# Scenario 4: versioned bucket with repeated overwrites
# ---------------------------------------------------------------------------

def cmd_scenario_4(args) -> None:
    log.info("=== Scenario 4: versioned bucket with repeated overwrites "
             "(index pool) ===")
    if args.disable_resharding:
        disable_dynamic_resharding()

    s3 = s3_client(args.endpoint, args.access_key, args.secret_key,
                   region=args.region, verify_tls=not args.insecure)
    bucket = args.bucket or f"{BUCKET_PREFIX}-4-{unique_suffix()}"

    create_bucket_single_shard(s3, bucket)
    if args.force_single_shard:
        force_single_shard_via_admin(bucket)

    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    log.info("Versioning enabled on %s", bucket)

    total = args.unique_keys * args.versions_per_key
    log.info(
        "Writing %d unique keys × %d versions each = %d index entries",
        args.unique_keys, args.versions_per_key, total,
    )

    writes = [(k, v) for k in range(args.unique_keys)
              for v in range(args.versions_per_key)]

    def _put(item) -> None:
        k, _v = item
        s3.put_object(Bucket=bucket, Key=f"key-{k:06d}", Body=b"")

    parallel_map(_put, writes,
                 parallelism=args.parallelism, desc="versioned PUTs")

    log.info("Scenario 4 complete. Bucket: %s", bucket)


# ---------------------------------------------------------------------------
# Scenario 5: delete marker accumulation
# ---------------------------------------------------------------------------

def cmd_scenario_5(args) -> None:
    log.info("=== Scenario 5: delete marker accumulation (index pool) ===")
    if args.disable_resharding:
        disable_dynamic_resharding()

    s3 = s3_client(args.endpoint, args.access_key, args.secret_key,
                   region=args.region, verify_tls=not args.insecure)
    bucket = args.bucket or f"{BUCKET_PREFIX}-5-{unique_suffix()}"

    create_bucket_single_shard(s3, bucket)
    if args.force_single_shard:
        force_single_shard_via_admin(bucket)

    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    log.info("Versioning enabled on %s", bucket)

    log.info("Phase 1/2: PUT %d objects", args.count)

    def _put(i: int) -> None:
        s3.put_object(Bucket=bucket, Key=f"key-{i:08d}", Body=b"")

    parallel_map(_put, list(range(args.count)),
                 parallelism=args.parallelism, desc="PUTs")

    log.info("Phase 2/2: DELETE %d objects (creates delete markers)",
             args.count)

    def _delete(i: int) -> None:
        # No VersionId -> creates a delete marker rather than removing.
        s3.delete_object(Bucket=bucket, Key=f"key-{i:08d}")

    parallel_map(_delete, list(range(args.count)),
                 parallelism=args.parallelism, desc="DELETEs")

    log.info("Scenario 5 complete. Bucket: %s "
             "(expect ~%d index entries: %d versions + %d delete markers)",
             bucket, args.count * 2, args.count, args.count)


# ---------------------------------------------------------------------------
# Scenario 7: many buckets per user (meta pool)
# ---------------------------------------------------------------------------

def cmd_scenario_7(args) -> None:
    log.info("=== Scenario 7: many buckets per user (meta pool) ===")

    # Raise the user's max-buckets if we can.
    if args.bump_max_buckets:
        ensure_user_via_admin(
            args.user, args.display_name,
            max_buckets=max(args.count + 1000, 100_000),
        )
    else:
        log.warning(
            "Skipping max-buckets bump. RGW will refuse creation past the "
            "user's max-buckets (default 1000). Pass --bump-max-buckets or "
            "raise it manually."
        )

    s3 = s3_client(args.endpoint, args.access_key, args.secret_key,
                   region=args.region, verify_tls=not args.insecure)

    suffix = unique_suffix(4)
    log.info("Creating %d buckets owned by %s (prefix: %s-7-%s-)",
             args.count, args.user, BUCKET_PREFIX, suffix)

    def _mkbucket(i: int) -> None:
        name = f"{BUCKET_PREFIX}-7-{suffix}-{i:07d}"
        try:
            s3.create_bucket(Bucket=name)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                return
            raise

    parallel_map(_mkbucket, list(range(args.count)),
                 parallelism=args.parallelism, desc="bucket creates")

    log.info("Scenario 7 complete. To verify the warning lands on the meta "
             "pool, force a deep scrub on `*.rgw.meta` PGs specifically.")


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

VERIFY_HELP = """
After populating OMAP keys you must force a deep scrub before the warning
appears. Run these on a Ceph admin node:

  # Identify the relevant pools
  ceph osd pool ls | grep rgw

  # Force deep scrub on every PG in the index pool
  for pg in $(ceph pg ls-by-pool default.rgw.buckets.index -f json \\
        | jq -r '.pg_stats[].pgid'); do
    ceph pg deep-scrub "$pg"
  done

  # ...and/or the meta pool
  for pg in $(ceph pg ls-by-pool default.rgw.meta -f json \\
        | jq -r '.pg_stats[].pgid'); do
    ceph pg deep-scrub "$pg"
  done

  # Wait for scrubs to finish, then:
  ceph health detail

  # To find the specific RADOS object and key count, search the cluster log:
  grep -i "Large omap object" /var/log/ceph/<fsid>/ceph.log

  # Or directly inspect a suspected index object:
  rados -p default.rgw.buckets.index listomapkeys <object-name> | wc -l
"""


def cmd_verify(args) -> None:
    print(VERIFY_HELP)
    if args.auto and have_command("ceph"):
        log.info("Auto-running `ceph health detail`:")
        run_cmd(["ceph", "health", "detail"], check=False)


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------

def cmd_cleanup(args) -> None:
    log.info("=== Cleanup: removing buckets with prefix %s-* ===", BUCKET_PREFIX)
    s3 = s3_client(args.endpoint, args.access_key, args.secret_key,
                   region=args.region, verify_tls=not args.insecure)

    resp = s3.list_buckets()
    targets = [b["Name"] for b in resp.get("Buckets", [])
               if b["Name"].startswith(BUCKET_PREFIX + "-")]
    log.info("Found %d candidate bucket(s)", len(targets))

    for bucket in targets:
        log.info("Cleaning bucket %s", bucket)
        try:
            # Abort in-flight multipart uploads
            paginator = s3.get_paginator("list_multipart_uploads")
            for page in paginator.paginate(Bucket=bucket):
                for u in page.get("Uploads", []) or []:
                    s3.abort_multipart_upload(
                        Bucket=bucket, Key=u["Key"], UploadId=u["UploadId"],
                    )
        except ClientError as e:
            log.warning("MPU cleanup failed on %s: %s", bucket, e)

        try:
            # Delete every version + every delete marker
            paginator = s3.get_paginator("list_object_versions")
            for page in paginator.paginate(Bucket=bucket):
                to_delete = []
                for v in (page.get("Versions") or []):
                    to_delete.append({"Key": v["Key"],
                                      "VersionId": v["VersionId"]})
                for m in (page.get("DeleteMarkers") or []):
                    to_delete.append({"Key": m["Key"],
                                      "VersionId": m["VersionId"]})
                for chunk_start in range(0, len(to_delete), 1000):
                    chunk = to_delete[chunk_start:chunk_start + 1000]
                    if chunk:
                        s3.delete_objects(
                            Bucket=bucket, Delete={"Objects": chunk},
                        )
        except ClientError as e:
            log.warning("Version cleanup failed on %s: %s", bucket, e)

        # Finally, drop the bucket itself
        try:
            s3.delete_bucket(Bucket=bucket)
        except ClientError as e:
            log.warning("delete_bucket failed on %s: %s", bucket, e)

    log.info(
        "Done. If you bumped max-buckets or disabled dynamic resharding, "
        "restore those manually: \n"
        "  ceph config rm client.rgw rgw_dynamic_resharding\n"
        "  radosgw-admin user modify --uid=%s --max-buckets=1000\n"
        "  %s tune-threshold --restore",
        args.user, sys.argv[0],
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def add_s3_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--endpoint", default=os.environ.get("RGW_ENDPOINT"),
                   help="RGW endpoint URL (env: RGW_ENDPOINT).")
    p.add_argument("--access-key", default=os.environ.get("RGW_ACCESS_KEY"),
                   help="S3 access key (env: RGW_ACCESS_KEY).")
    p.add_argument("--secret-key", default=os.environ.get("RGW_SECRET_KEY"),
                   help="S3 secret key (env: RGW_SECRET_KEY).")
    p.add_argument("--region", default=DEFAULT_REGION)
    p.add_argument("--insecure", action="store_true",
                   help="Skip TLS verification (HTTPS endpoints only).")


def add_common_scenario_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--count", type=int, default=DEFAULT_COUNT,
                   help="Number of items to create (default: %(default)s).")
    p.add_argument("--parallelism", type=int, default=DEFAULT_PARALLELISM,
                   help="Concurrent workers (default: %(default)s).")
    p.add_argument("--bucket",
                   help="Use this specific bucket name (default: generated).")
    p.add_argument("--disable-resharding", action="store_true",
                   help="Run `ceph config set client.rgw rgw_dynamic_resharding "
                        "false` before starting. Requires ceph admin tooling "
                        "and an RGW restart.")
    p.add_argument("--force-single-shard", action="store_true",
                   help="Reshard the bucket down to 1 shard via radosgw-admin "
                        "(concentrates all keys in one OMAP object).")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="large_omap_repro.py",
        description="Reproduce LARGE_OMAP_OBJECTS warnings on Ceph RGW.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="scenario", required=True)

    # tune-threshold
    p = sub.add_parser("tune-threshold",
                       help="Lower OSD large-omap thresholds (scenario 2).")
    p.add_argument("--keys", type=int, default=5000,
                   help="osd_deep_scrub_large_omap_object_key_threshold "
                        "(default: %(default)s).")
    p.add_argument("--value-bytes", type=int, default=None,
                   help="osd_deep_scrub_large_omap_object_value_sum_threshold. "
                        "Omit to leave at default.")
    p.add_argument("--restore", action="store_true",
                   help="Remove the overrides and restore defaults.")
    p.set_defaults(func=cmd_tune_threshold)

    # scenario-1
    p = sub.add_parser("scenario-1",
                       help="Single-shard bucket saturation (index pool).")
    add_s3_args(p)
    add_common_scenario_args(p)
    p.set_defaults(func=cmd_scenario_1)

    # scenario-3
    p = sub.add_parser("scenario-3",
                       help="Many incomplete multipart uploads (index pool).")
    add_s3_args(p)
    add_common_scenario_args(p)
    p.set_defaults(func=cmd_scenario_3)

    # scenario-4
    p = sub.add_parser("scenario-4",
                       help="Versioned bucket repeated overwrites (index pool).")
    add_s3_args(p)
    add_common_scenario_args(p)
    p.add_argument("--unique-keys", type=int, default=60,
                   help="Distinct keys to overwrite (default: %(default)s).")
    p.add_argument("--versions-per-key", type=int, default=100,
                   help="Versions to write per key (default: %(default)s). "
                        "Total entries = unique-keys * versions-per-key.")
    p.set_defaults(func=cmd_scenario_4)

    # scenario-5
    p = sub.add_parser("scenario-5",
                       help="Delete marker accumulation (index pool).")
    add_s3_args(p)
    add_common_scenario_args(p)
    p.set_defaults(func=cmd_scenario_5)

    # scenario-7
    p = sub.add_parser("scenario-7",
                       help="Many buckets owned by one user (meta pool).")
    add_s3_args(p)
    add_common_scenario_args(p)
    p.add_argument("--user", default=DEFAULT_USER,
                   help="RGW user uid that will own the buckets "
                        "(default: %(default)s).")
    p.add_argument("--display-name", default=DEFAULT_DISPLAY_NAME)
    p.add_argument("--bump-max-buckets", action="store_true",
                   help="Run `radosgw-admin user modify --max-buckets=...` "
                        "before creating buckets.")
    p.set_defaults(func=cmd_scenario_7)

    # verify
    p = sub.add_parser("verify",
                       help="Print verification commands; optionally run "
                            "`ceph health detail`.")
    p.add_argument("--auto", action="store_true",
                   help="Also run `ceph health detail` if ceph is available.")
    p.set_defaults(func=cmd_verify)

    # cleanup
    p = sub.add_parser("cleanup",
                       help="Remove buckets this script created.")
    add_s3_args(p)
    p.add_argument("--user", default=DEFAULT_USER,
                   help="User whose buckets you want to clean (only used in "
                        "the reminder message at the end).")
    p.set_defaults(func=cmd_cleanup)

    return parser


def validate_s3_args(args) -> None:
    """Ensure S3 args are set for scenarios that need them."""
    needed = ("endpoint", "access_key", "secret_key")
    missing = [n for n in needed if not getattr(args, n, None)]
    if missing:
        log.error(
            "Missing S3 settings: %s. Pass --%s or set RGW_%s env vars.",
            ", ".join(missing),
            ", --".join(m.replace("_", "-") for m in missing),
            ", RGW_".join(m.upper() for m in missing),
        )
        sys.exit(2)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Scenarios that talk to S3 need credentials.
    if args.scenario in {"scenario-1", "scenario-3", "scenario-4",
                         "scenario-5", "scenario-7", "cleanup"}:
        validate_s3_args(args)

    args.func(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Interrupted.")
        sys.exit(130)
