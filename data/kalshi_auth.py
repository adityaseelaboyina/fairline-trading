"""Kalshi RSA API-key request signing.

Implemented against docs.kalshi.com as of 2026-06-23 (see docs/API_NOTES.md):
signed message = timestamp_ms + METHOD + path(no query), RSA-PSS / SHA-256 /
salt=digest-length, base64-encoded. Key material is loaded from env-specified
paths; nothing secret is hardcoded.
"""
from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


def _now_ms() -> str:
    return str(int(time.time() * 1000))


@dataclass
class KalshiSigner:
    """Builds the KALSHI-ACCESS-* headers for a request."""

    api_key_id: str
    _private_key: rsa.RSAPrivateKey

    @classmethod
    def from_config(cls, cfg: dict) -> "KalshiSigner":
        kc = cfg["kalshi"]
        key_id = os.environ.get(kc["api_key_id_env"])
        key_path = os.environ.get(kc["private_key_path_env"])
        if not key_id or not key_path:
            raise RuntimeError(
                f"Set {kc['api_key_id_env']} and {kc['private_key_path_env']} in the "
                "environment (the latter pointing at your RSA private key .pem)."
            )
        with open(key_path, "rb") as fh:
            pk = serialization.load_pem_private_key(fh.read(), password=None)
        if not isinstance(pk, rsa.RSAPrivateKey):
            raise RuntimeError("Loaded key is not an RSA private key.")
        return cls(api_key_id=key_id, _private_key=pk)

    def _sign(self, message: str) -> str:
        sig = self._private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    def headers(self, method: str, path: str) -> dict:
        """Headers for a REST call. `path` must include the /trade-api/v2 prefix
        and exclude the query string."""
        ts = _now_ms()
        path_no_query = path.split("?", 1)[0]
        signature = self._sign(f"{ts}{method.upper()}{path_no_query}")
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": signature,
        }

    def ws_headers(self, sign_path: str) -> dict:
        """Headers for the WS handshake (signs `ts + 'GET' + sign_path`)."""
        return self.headers("GET", sign_path)
