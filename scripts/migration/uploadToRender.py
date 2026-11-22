#!/usr/bin/env python3
"""GitHub Actions side of the cutover transfer: send a verified archive to the
Render restore receiver and wait for its reconciliation verdict.

usage: uploadToRender.py <archive.dump> <rehearsal|final>
Auth: a fresh GitHub OIDC token (audience pulseforge-render-migration) on every
request. Needs `permissions: id-token: write`. Logs counts and checksums only.
"""
from __future__ import annotations

import json
import os
import sys
import time

import requests

RECEIVER = "https://pulseforge-api-migration.onrender.com"
AUDIENCE = "pulseforge-render-migration"
CHUNK = 32 * 1024 * 1024
POLL_SECONDS = int(os.environ.get("RESTORE_POLL_SECONDS", "5400"))


def log(status, detail=None):
    print("UPLOAD_" + status + (" " + json.dumps(detail, sort_keys=True) if detail else ""), flush=True)


def token() -> str:
    url, bearer = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL"), os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    if not url or not bearer:
        raise RuntimeError("oidc_unavailable_need_id_token_write")
    r = requests.get(url, params={"audience": AUDIENCE}, headers={"Authorization": "bearer " + bearer}, timeout=30)
    r.raise_for_status()
    return r.json()["value"]


def call(method, path, *, retries=4, **kw):
    extra = kw.pop("headers", {})
    for attempt in range(retries):
        try:
            r = requests.request(method, RECEIVER + path, headers={"Authorization": "Bearer " + token(), **extra},
                                 timeout=(20, 600), allow_redirects=False, **kw)
            if r.status_code in (502, 503, 504) and attempt + 1 < retries:
                time.sleep(10 * (attempt + 1))
                continue
            return r
        except requests.RequestException:
            if attempt + 1 == retries:
                raise
            time.sleep(10 * (attempt + 1))
    raise RuntimeError("unreachable")


def main(argv) -> int:
    try:
        archive, mode = argv[1], argv[2]
        if mode not in ("rehearsal", "final"):
            raise RuntimeError("bad_mode")
        with open(archive + ".manifest.json", "rb") as f:
            manifest = json.load(f)
        if manifest["bytes"] != os.path.getsize(archive):
            raise RuntimeError("local_archive_size_mismatch")
        r = call("GET", "/healthz", retries=12)
        if r.status_code != 200 or r.json().get("receiver") != "ready":
            raise RuntimeError("receiver_not_ready_%d" % r.status_code)
        r = call("POST", "/internal/migration/upload/begin", json={"mode": mode, "manifest": manifest}, retries=1)
        if r.status_code != 200:
            raise RuntimeError("begin_refused_%d_%s" % (r.status_code, r.text[:120]))
        sent, index = 0, 0
        with open(archive, "rb") as f:
            while True:
                data = f.read(CHUNK)
                if not data:
                    break
                r = call("POST", "/internal/migration/upload/chunk?index=%d" % index, data=data,
                         headers={"Content-Type": "application/octet-stream"}, retries=1)
                if r.status_code != 200:
                    raise RuntimeError("chunk_%d_refused_%d_%s" % (index, r.status_code, r.text[:120]))
                sent += len(data)
                index += 1
                if index % 5 == 0:
                    log("PROGRESS", {"bytes": sent, "of": manifest["bytes"]})
        r = call("POST", "/internal/migration/upload/complete", retries=1)
        if r.status_code != 202:
            raise RuntimeError("complete_refused_%d_%s" % (r.status_code, r.text[:160]))
        log("SENT", {"bytes": sent, "chunks": index, "sha256": manifest["sha256"]})
        deadline = time.time() + POLL_SECONDS
        while time.time() < deadline:
            time.sleep(20)
            r = call("GET", "/internal/migration/status", retries=6)
            if r.status_code != 200:
                continue
            st = r.json()
            if st.get("phase") in ("complete", "failed"):
                log("RECEIVER_RESULT", st)
                return 0 if st["phase"] == "complete" else 1
        raise RuntimeError("receiver_verdict_timeout")
    except Exception as exc:  # noqa: BLE001
        log("FAILED", {"reason": str(exc)[:300]})
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
