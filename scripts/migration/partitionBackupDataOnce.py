#!/usr/bin/env python3
"""One-time offline pg_restore table-data partition validation. Does not restore to a DB."""
from __future__ import annotations
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
import boto3
from botocore.config import Config

EXPECTED_KEY = "migration-backups/railway-20261117-full-pg18.dump"
EXPECTED_SIZE = 727898203
EXPECTED_SHA = "4d44ad1f73ffcb5a3ee48ee8b30c78c2430f803da05915b40b84b6101a736628"
CATALOG = "/usr/local/share/migration/railway-source-catalog-20261117.json"
TOC_PATTERN = re.compile(r"^\d+;\s+\d+\s+\d+\s+TABLE DATA\s+(\S+)\s+(\S+)(?:\s|$)")
ALLOW = {"PULSEFORGE", "SIGNAL"}

def main():
    if os.getenv("PARTITION_CHECK_APPROVED") != "OFFLINE_DECOMPRESS_ONLY":
        raise RuntimeError("partition_check_not_approved")
    names = ["S3_ENDPOINT", "S3_BUCKET", "S3_REGION", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"]
    if any(not os.getenv(name) for name in names):
        raise RuntimeError("missing_bucket_credentials")
    catalog = json.loads(Path(CATALOG).read_text())
    source = {f'{x["schema_name"]}.{x["table_name"]}' for x in catalog["objects"]
              if x["relkind"] in ("r","p")}
    if len(source) != 319:
        raise RuntimeError("catalog_table_count_mismatch")
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
        region_name=os.environ["S3_REGION"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        config=Config(s3={"addressing_style": "virtual"}))
    with tempfile.TemporaryDirectory(prefix="migration-partition-") as folder:
        dump = Path(folder) / "railway.dump"
        checksum = hashlib.sha256()
        count=0
        resp=s3.get_object(Bucket=os.environ["S3_BUCKET"], Key=EXPECTED_KEY)
        body=resp["Body"]
        try:
            with dump.open("wb") as out:
                for block in body.iter_chunks(chunk_size=4*1024*1024):
                    if block:
                        count += len(block)
                        if count > EXPECTED_SIZE: raise RuntimeError("oversize_object")
                        checksum.update(block)
                        out.write(block)
        finally:
            body.close()
        if count!=EXPECTED_SIZE or checksum.hexdigest()!=EXPECTED_SHA:
            raise RuntimeError("backup_sha_or_size_mismatch")
        toc=subprocess.run(["pg_restore","--list",str(dump)],capture_output=True,text=True,
                           timeout=120,check=True).stdout.splitlines()
        selected={"PULSEFORGE":set(),"SIGNAL":set()}
        lines={"PULSEFORGE":[],"SIGNAL":[]}
        for line in toc:
            match=TOC_PATTERN.match(line)
            name=match.group(1)+"."+match.group(2) if match else None
            if name:
                if name not in source: raise RuntimeError("archive_table_not_in_catalog")
                group="SIGNAL" if match.group(2).startswith("signal_") else "PULSEFORGE"
                if name in selected[group]: raise RuntimeError("duplicate_table_data_entry")
                selected[group].add(name)
            for group in ALLOW:
                lines[group].append(line if name and name in selected[group] else ";" + line)
        union=selected["PULSEFORGE"]|selected["SIGNAL"]
        if union!=source or selected["PULSEFORGE"]&selected["SIGNAL"]:
            raise RuntimeError("archive_catalog_mismatch")
        if len(selected["PULSEFORGE"])!=274 or len(selected["SIGNAL"])!=45:
            raise RuntimeError("unexpected_partition_size")
        print("PARTITION_TOC_MATCHED "+json.dumps({
            "sourceTables":len(source),"pulseforgeTables":len(selected["PULSEFORGE"]),
            "signalTables":len(selected["SIGNAL"]),"crossingTableDataEntries":0,
            "backupSha256":EXPECTED_SHA,"status":"PRELIMINARY_NOT_VALIDATED_SCHEMA_SPLIT"
        },sort_keys=True),flush=True)
        for group in ("PULSEFORGE","SIGNAL"):
            toc_path=Path(folder)/(group.lower()+".toc")
            toc_path.write_text("\n".join(lines[group])+"\n")
            # Reads and decompresses all selected TABLE DATA archive members to /dev/null.
            proc=subprocess.run(["pg_restore","--data-only","--use-list",str(toc_path),
                                 "--file","/dev/null",str(dump)],
                                stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                                timeout=900)
            if proc.returncode:
                raise RuntimeError("archive_decompression_failed_"+group+"_"+str(proc.returncode))
            print("PARTITION_DATA_DECOMPRESSED "+json.dumps({
                "group":group,"tableDataEntries":len(selected[group]),
                "status":"VALIDATED_ARCHIVE_DECOMPRESSION_NO_DATABASE_RESTORE"
            },sort_keys=True),flush=True)
    print("PARTITION_CHECK_COMPLETE",flush=True)

if __name__=="__main__":
    try: main()
    except Exception as e:
        print("PARTITION_CHECK_FAILED "+
              (str(e) if isinstance(e,RuntimeError) else type(e).__name__),
              file=sys.stderr,flush=True)
        raise SystemExit(1)
