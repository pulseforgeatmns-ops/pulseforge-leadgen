"""Cutover transfer guards: GitHub OIDC gate and receiver manifest rules.

python3 -m unittest test/test_cutoverTransfer.py  (needs python3-cryptography)
The full export -> freeze -> upload -> restore -> swap -> unfreeze path was
exercised against disposable PostgreSQL 18 (docs/migration/CUTOVER_EXECUTION_20261122.md).
"""
import base64
import json
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts", "migration"))

try:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding, rsa
except ImportError:  # pragma: no cover
    rsa = None

import githubOidc  # noqa: E402


def b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


@unittest.skipIf(rsa is None, "cryptography not installed")
class OidcGate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        n = cls.key.public_key().public_numbers()
        cls.jwks = {"keys": [{"kty": "RSA", "kid": "k1", "n": b64(n.n.to_bytes(256, "big")),
                              "e": b64(n.e.to_bytes(3, "big"))}]}

    def mint(self, key=None, **over):
        now = int(time.time())
        claims = {"iss": githubOidc.ISSUER, "aud": githubOidc.AUDIENCE, "exp": now + 300, "nbf": now - 5,
                  "repository": githubOidc.REPOSITORY, "ref": githubOidc.REF, "event_name": "push",
                  "workflow_ref": "%s/%s@%s" % (githubOidc.REPOSITORY, githubOidc.WORKFLOW_PATH, githubOidc.REF),
                  "run_id": "42"}
        claims.update(over)
        head = b64(json.dumps({"alg": "RS256", "kid": "k1"}).encode())
        body = b64(json.dumps(claims).encode())
        sig = (key or self.key).sign((head + "." + body).encode(), padding.PKCS1v15(), hashes.SHA256())
        return head + "." + body + "." + b64(sig)

    def verify(self, token):
        return githubOidc.verify(token, jwks=self.jwks)

    def test_accepts_pinned_workflow_on_pinned_branch(self):
        self.assertEqual(self.verify(self.mint())["run_id"], "42")

    def test_refuses_everything_else(self):
        other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        main_ref = "refs/heads/main"
        cases = {
            "bad_signature": self.mint(key=other),
            "wrong_issuer": self.mint(iss="https://evil.example"),
            "wrong_audience": self.mint(aud="someone-else"),
            "expired": self.mint(exp=int(time.time()) - 600),
            "wrong_repository": self.mint(repository="someone/fork"),
            "wrong_ref": self.mint(ref=main_ref),
            "wrong_workflow": self.mint(workflow_ref="%s/.github/workflows/other.yml@%s"
                                        % (githubOidc.REPOSITORY, githubOidc.REF)),
            "wrong_event": self.mint(event_name="pull_request"),
        }
        for reason, token in cases.items():
            with self.subTest(reason=reason):
                with self.assertRaises(githubOidc.OidcError) as ctx:
                    self.verify(token)
                self.assertEqual(str(ctx.exception), reason)
        with self.assertRaises(githubOidc.OidcError):
            self.verify("not.a.jwt")


@unittest.skipIf(rsa is None, "cryptography not installed")
class ManifestRules(unittest.TestCase):
    def setUp(self):
        try:
            import restoreFinalRender  # needs psycopg2
        except ImportError:
            self.skipTest("psycopg2 not installed")
        self.r = restoreFinalRender
        self.m = {"sha256": "a" * 64, "bytes": 4096, "serverVersionNum": 180006, "fingerprints": {},
                  "sequences": {}, "catalog": {}, "signalTables": [], "snapshotCapturedAt": "x"}

    def test_final_requires_frozen_source(self):
        self.r.validate_manifest(dict(self.m), "rehearsal")
        with self.assertRaisesRegex(ValueError, "final_requires_frozen_source_snapshot"):
            self.r.validate_manifest(dict(self.m), "final")
        self.r.validate_manifest(dict(self.m, sourceFrozen=True), "final")

    def test_rejects_non_pg18_and_pinned_sha_mismatch(self):
        with self.assertRaisesRegex(ValueError, "source_not_postgres18"):
            self.r.validate_manifest(dict(self.m, serverVersionNum=170004), "rehearsal")
        os.environ["EXPECTED_ARCHIVE_SHA256"] = "b" * 64
        try:
            with self.assertRaisesRegex(ValueError, "pinned"):
                self.r.validate_manifest(dict(self.m), "rehearsal")
        finally:
            del os.environ["EXPECTED_ARCHIVE_SHA256"]


if __name__ == "__main__":
    unittest.main()
