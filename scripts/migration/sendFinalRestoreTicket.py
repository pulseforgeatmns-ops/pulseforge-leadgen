#!/usr/bin/env python3
"""One-shot Railway sender: presigned dump + manifest URLs to the Render receiver.

Never logs the URLs, reads the object, or connects to PostgreSQL. The Render
receiver checks the archive against its operator-pinned SHA-256.
"""
import os
import sys
from urllib.parse import urlsplit

import boto3
import requests
from botocore.config import Config

RECEIVER_BASE = "https://pulseforge-api-migration.onrender.com"


def main():
    if os.getenv("SEND_FINAL_TICKET_APPROVED") != "ONE_TIME_RENDER_RESTORE":
        raise RuntimeError("ticket_not_approved")
    names = ("S3_ENDPOINT", "S3_BUCKET", "S3_REGION", "S3_ACCESS_KEY_ID",
             "S3_SECRET_ACCESS_KEY", "MIGRATION_BRIDGE_TOKEN", "BACKUP_OBJECT_KEY", "RESTORE_MODE")
    if any(not os.getenv(n) for n in names):
        raise RuntimeError("missing_reference_or_token")
    key, mode = os.environ["BACKUP_OBJECT_KEY"], os.environ["RESTORE_MODE"]
    if not key.startswith("migration-backups/") or not key.endswith(".dump") or ".." in key:
        raise RuntimeError("invalid_backup_object_key")
    if mode not in ("rehearsal", "final"):
        raise RuntimeError("invalid_restore_mode")
    if len(os.environ["MIGRATION_BRIDGE_TOKEN"]) < 45:
        raise RuntimeError("invalid_bridge_token")
    if urlsplit(os.environ["S3_ENDPOINT"]).hostname != "t3.storageapi.dev":
        raise RuntimeError("unrecognized_s3_endpoint")
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                      aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
                      region_name=os.environ["S3_REGION"],
                      config=Config(s3={"addressing_style": "virtual", "signature_version": "s3v4"}))
    bucket = os.environ["S3_BUCKET"]
    s3.head_object(Bucket=bucket, Key=key)
    s3.head_object(Bucket=bucket, Key=key + ".manifest.json")
    health = requests.get(RECEIVER_BASE + "/healthz", allow_redirects=False, timeout=(12, 25))
    if health.status_code != 200:
        raise RuntimeError("render_receiver_not_ready_%d" % health.status_code)
    sign = lambda k: s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": k}, ExpiresIn=3600)
    result = requests.post(RECEIVER_BASE + "/internal/migration/restore-final",
                           json={"downloadUrl": sign(key), "manifestUrl": sign(key + ".manifest.json"), "mode": mode},
                           headers={"Authorization": "Bearer " + os.environ["MIGRATION_BRIDGE_TOKEN"]},
                           allow_redirects=False, timeout=(12, 30))
    if result.status_code != 202:
        raise RuntimeError("render_receiver_rejected_%d" % result.status_code)
    print("FINAL_TICKET_ACCEPTED_202 mode=" + mode, flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("FINAL_TICKET_FAILED " + (str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__),
              file=sys.stderr, flush=True)
        sys.exit(1)
