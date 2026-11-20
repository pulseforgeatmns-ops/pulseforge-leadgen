#!/usr/bin/env python3
"""Send an authenticated, short-lived presigned backup URL to Render staging.

Does not read or log the object, copy credentials, connect to PostgreSQL,
or change the source database. The Render receiver performs its own SHA check.
"""
import os
import sys
from urllib.parse import urlsplit
import boto3
import requests
from botocore.config import Config

ARCHIVE_KEY = "migration-backups/railway-20261117-full-pg18.dump"
RECEIVER = "https://pulseforge-api-migration.onrender.com/internal/migration/restore-once"

def main():
    if os.getenv("SEND_RESTORE_TICKET_APPROVED") != "ONE_TIME_RENDER_STAGING_ONLY":
        raise RuntimeError("bridge_not_approved")
    names = ("S3_ENDPOINT","S3_BUCKET","S3_REGION",
             "S3_ACCESS_KEY_ID","S3_SECRET_ACCESS_KEY","MIGRATION_BRIDGE_TOKEN")
    if any(not os.getenv(name) for name in names):
        raise RuntimeError("missing_reference_or_token")
    token = os.environ["MIGRATION_BRIDGE_TOKEN"]
    if len(token) < 45:
        raise RuntimeError("invalid_bridge_token")
    if urlsplit(os.environ["S3_ENDPOINT"]).hostname != "t3.storageapi.dev":
        raise RuntimeError("unrecognized_s3_endpoint")
    s3=boto3.client("s3",endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        region_name=os.environ["S3_REGION"],
        config=Config(s3={"addressing_style":"virtual",
                           "signature_version":"s3v4"}))
    health=requests.get("https://pulseforge-api-migration.onrender.com/healthz",
        allow_redirects=False,timeout=(12,25))
    if health.status_code != 200:
        raise RuntimeError("render_staging_health_not_ready_"+str(health.status_code))
    url=s3.generate_presigned_url("get_object",
        Params={"Bucket":os.environ["S3_BUCKET"],"Key":ARCHIVE_KEY},ExpiresIn=3600)
    # One tiny authenticated message; never emit the sensitive URL to any log.
    result=requests.post(RECEIVER,json={"downloadUrl":url},
        headers={"Authorization":"Bearer "+token},
        allow_redirects=False,timeout=(12,30))
    if result.status_code != 202:
        raise RuntimeError("render_receiver_rejected_ticket_status_"+str(result.status_code))
    print("RENDER_RESTORE_TICKET_ACCEPTED_202",flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("RENDER_TICKET_FAILED "+(
            str(exc) if isinstance(exc,RuntimeError) else type(exc).__name__
        ),file=sys.stderr,flush=True)
        sys.exit(1)
