"""Verify a GitHub Actions OIDC token for the cutover upload. No shared secret.

The Render receiver accepts an upload only from the railway-final-export
workflow of this repository, running on the pinned cutover branch. GitHub
signs the token (RS256); keys come from GitHub's published JWKS.
"""
from __future__ import annotations

import base64
import json
import time

ISSUER = "https://token.actions.githubusercontent.com"
JWKS_URL = ISSUER + "/.well-known/jwks"
AUDIENCE = "pulseforge-render-migration"
REPOSITORY = "pulseforgeatmns-ops/pulseforge-leadgen"
REF = "refs/heads/claude/pulseforge-railway-render-cutover-m1r15e"
WORKFLOW_PATH = ".github/workflows/railway-final-export.yml"
CLOCK_SKEW = 60


class OidcError(Exception):
    pass


def _b64(part: str) -> bytes:
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))


def _fetch_jwks():
    import requests
    r = requests.get(JWKS_URL, timeout=(10, 20), allow_redirects=False)
    if r.status_code != 200:
        raise OidcError("jwks_unavailable")
    return r.json()


def _public_key(jwk: dict):
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
    if jwk.get("kty") != "RSA":
        raise OidcError("unexpected_key_type")
    n = int.from_bytes(_b64(jwk["n"]), "big")
    e = int.from_bytes(_b64(jwk["e"]), "big")
    return RSAPublicNumbers(e, n).public_key()


def verify(token: str, *, jwks=None, now=None) -> dict:
    """Return the verified claims, or raise OidcError. Never logs the token."""
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    try:
        head_b64, body_b64, sig_b64 = token.split(".")
        header = json.loads(_b64(head_b64))
        claims = json.loads(_b64(body_b64))
        signature = _b64(sig_b64)
    except Exception as exc:  # noqa: BLE001
        raise OidcError("malformed_token") from exc
    if header.get("alg") != "RS256" or not header.get("kid"):
        raise OidcError("unexpected_algorithm")
    keys = (jwks or _fetch_jwks()).get("keys", [])
    match = [k for k in keys if k.get("kid") == header["kid"]]
    if len(match) != 1:
        raise OidcError("unknown_signing_key")
    try:
        _public_key(match[0]).verify(signature, (head_b64 + "." + body_b64).encode(),
                                     padding.PKCS1v15(), hashes.SHA256())
    except InvalidSignature as exc:
        raise OidcError("bad_signature") from exc
    t = time.time() if now is None else now
    if claims.get("iss") != ISSUER:
        raise OidcError("wrong_issuer")
    aud = claims.get("aud")
    if aud != AUDIENCE and not (isinstance(aud, list) and AUDIENCE in aud):
        raise OidcError("wrong_audience")
    if not isinstance(claims.get("exp"), (int, float)) or claims["exp"] < t - CLOCK_SKEW:
        raise OidcError("expired")
    if isinstance(claims.get("nbf"), (int, float)) and claims["nbf"] > t + CLOCK_SKEW:
        raise OidcError("not_yet_valid")
    if claims.get("repository") != REPOSITORY:
        raise OidcError("wrong_repository")
    if claims.get("ref") != REF:
        raise OidcError("wrong_ref")
    if claims.get("workflow_ref") != "%s/%s@%s" % (REPOSITORY, WORKFLOW_PATH, REF):
        raise OidcError("wrong_workflow")
    if claims.get("event_name") != "push":
        raise OidcError("wrong_event")
    if not str(claims.get("run_id", "")).isdigit():
        raise OidcError("missing_run_id")
    return claims
