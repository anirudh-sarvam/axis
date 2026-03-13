#!/usr/bin/env python3
"""
Axis Bank CHUB (Channel Hub) API Integration Test Client

Implements the full JWE/JWS encryption pipeline for Axis Bank's CHUB gateway:

  Request flow:
    plaintext → JWE(RSA-OAEP-256, A256GCM) → JWS(RS256) → HTTPS+mTLS → CHUB

  Response flow:
    CHUB → HTTPS+mTLS → JWS verify(RS256) → JWE decrypt(RSA-OAEP-256) → plaintext

Auth via X-IBM-Client-Id / X-IBM-Client-Secret headers + mTLS (client .p12).

Usage:
    # Local crypto round-trip (no network, no private key required)
    python chub_test.py --self-test

    # Send message via CHUB (uses default endpoint + certs/sarvam.p12)
    python chub_test.py

    # Custom endpoint
    python chub_test.py --endpoint /axis/uat-nondmz/api/v1/partnerai/chub/outbound/send-message

    # Custom payload
    python chub_test.py --payload '{"Data":{...},"Risk":{}}'
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import ssl
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from jwcrypto import jwe, jwk, jws
from jwcrypto.common import json_encode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("chub")


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

_BASE_DIR = Path('/home/sarvam/axis')

_CERTS_DIR = _BASE_DIR / "certs"

AXIS_ENCRYPTION_CERT_PATH = _CERTS_DIR / "rgw.jwejws.uat.axisb.com-sscert.txt"
P12_PATH = _CERTS_DIR / "sarvam.p12"
P12_PASSWORD = ""

# Legacy separate-file paths (fallback when .p12 is not available)
CLIENT_CERT_PATH = _CERTS_DIR / "SARVAM-client-certificate.txt"
CLIENT_KEY_PATH = _CERTS_DIR / "axis_private.key"
ROOT_CA_PATH = _CERTS_DIR / "UATRoot_Cert.txt"
INTERMEDIATE_CA_PATH = _CERTS_DIR / "UATIntermediate_Cert.txt"

CLIENT_ID = "1f42ebc532ab4b08f19d75209e0ceca1"
CLIENT_SECRET = "6f04441c1b790d40bb2f41fb4c616bd1"
CHANNEL_ID = "SARVAM"
UAT_BASE_URL = "https://nondmz-gateway.uat.axisb.com"
DEFAULT_ENDPOINT = "/axis/uat-nondmz/api/v1/partnerai/chub/outbound/send-message"


@dataclass
class CHUBConfig:
    """All parameters needed to connect to and communicate with Axis CHUB."""

    client_id: str = CLIENT_ID
    client_secret: str = CLIENT_SECRET
    channel_id: str = CHANNEL_ID
    base_url: str = UAT_BASE_URL
    default_endpoint: str = DEFAULT_ENDPOINT

    axis_encryption_cert_path: Path = AXIS_ENCRYPTION_CERT_PATH
    p12_path: Optional[Path] = P12_PATH
    p12_password: Optional[str] = P12_PASSWORD

    # Legacy separate-file paths (used when .p12 is not available)
    client_cert_path: Optional[Path] = CLIENT_CERT_PATH
    client_key_path: Optional[Path] = CLIENT_KEY_PATH
    root_ca_path: Optional[Path] = ROOT_CA_PATH
    intermediate_ca_path: Optional[Path] = INTERMEDIATE_CA_PATH

    mode: str = "asymmetric"  # "asymmetric" | "symmetric"


# ─────────────────────────────────────────────────────────────────────────────
# Crypto Utilities — mirrors the Java EncryptionUtils / CertificateUtils
# ─────────────────────────────────────────────────────────────────────────────

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


class CryptoUtils:
    """JWE/JWS operations matching Axis CHUB's Visa reference implementation."""

    # ── Key loading ──────────────────────────────────────────────────────

    @staticmethod
    def load_public_key_from_cert(cert_path: Path) -> jwk.JWK:
        pem_data = cert_path.read_bytes()
        cert = x509.load_pem_x509_certificate(pem_data)
        pub_pem = cert.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        return jwk.JWK.from_pem(pub_pem)

    @staticmethod
    def load_private_key(key_path: Path, password: Optional[bytes] = None) -> jwk.JWK:
        pem_data = key_path.read_bytes()
        return jwk.JWK.from_pem(pem_data, password=password)

    @staticmethod
    def generate_test_keypair(key_size: int = 4096) -> tuple[jwk.JWK, jwk.JWK]:
        """Ephemeral RSA keypair for local self-tests."""
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=key_size
        )
        priv_pem = private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        pub_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        return jwk.JWK.from_pem(priv_pem), jwk.JWK.from_pem(pub_pem)

    # ── Symmetric helpers ────────────────────────────────────────────────

    @staticmethod
    def _sha256_oct_key(shared_secret: str) -> jwk.JWK:
        """SHA-256 of the shared secret → 256-bit AES key (for JWE A256GCMKW).
        Mirrors: Java MessageDigest.getInstance("SHA-256").digest(secret.getBytes(UTF-8))"""
        digest = hashlib.sha256(shared_secret.encode("utf-8")).digest()
        return jwk.JWK(kty="oct", k=_b64url(digest))

    @staticmethod
    def _raw_oct_key(shared_secret: str) -> jwk.JWK:
        """Raw UTF-8 bytes of the shared secret → HMAC key (for JWS HS256).
        Mirrors: Java MACSigner(sharedSecret.getBytes(UTF-8))"""
        raw = shared_secret.encode("utf-8")
        return jwk.JWK(kty="oct", k=_b64url(raw))

    # ── Asymmetric JWE (RSA-OAEP-256 + A256GCM) ─────────────────────────

    @staticmethod
    def create_jwe_rsa(
        plaintext: str,
        public_key: jwk.JWK,
        extra_headers: Optional[dict[str, Any]] = None,
    ) -> str:
        protected_header: dict[str, Any] = {
            "alg": "RSA-OAEP-256",
            "enc": "A256GCM",
        }
        if extra_headers:
            protected_header.update(extra_headers)

        token = jwe.JWE(
            plaintext.encode("utf-8"),
            recipient=public_key,
            protected=json_encode(protected_header),
        )
        return token.serialize(compact=True)

    @staticmethod
    def decrypt_jwe_rsa(jwe_string: str, private_key: jwk.JWK) -> str:
        token = jwe.JWE()
        token.deserialize(jwe_string, key=private_key)
        return token.payload.decode("utf-8")

    # ── Asymmetric JWS (RS256) ───────────────────────────────────────────

    @staticmethod
    def create_jws_rsa(
        payload: str,
        private_key: jwk.JWK,
        extra_headers: Optional[dict[str, Any]] = None,
    ) -> str:
        protected_header: dict[str, Any] = {
            "alg": "RS256",
        }
        if extra_headers:
            protected_header.update(extra_headers)

        token = jws.JWS(payload.encode("utf-8"))
        token.add_signature(private_key, protected=json_encode(protected_header))
        return token.serialize(compact=True)

    @staticmethod
    def verify_and_extract_jws_rsa(jws_string: str, public_key: jwk.JWK) -> str:
        """Mirrors: EncryptionUtils.verifyAndExtractJweFromJWS(jws, publicKey)"""
        token = jws.JWS()
        token.deserialize(jws_string)
        token.verify(public_key)
        return token.payload.decode("utf-8")

    # ── Symmetric JWE (A256GCMKW + A256GCM) ─────────────────────────────

    @classmethod
    def create_jwe_symmetric(
        cls,
        plaintext: str,
        api_key: str,
        shared_secret: str,
        extra_headers: Optional[dict[str, Any]] = None,
    ) -> str:
        """Mirrors: EncryptionUtils.createJwe(plain, apiKey, sharedSecret, A256GCMKW, A256GCM, headers)"""
        protected_header: dict[str, Any] = {
            "alg": "A256GCMKW",
            "enc": "A256GCM",
            "kid": api_key,
            "typ": "JOSE",
        }
        if extra_headers:
            protected_header.update(extra_headers)

        key = cls._sha256_oct_key(shared_secret)
        token = jwe.JWE(
            plaintext.encode("utf-8"),
            recipient=key,
            protected=json_encode(protected_header),
        )
        return token.serialize(compact=True)

    @classmethod
    def decrypt_jwe_symmetric(cls, jwe_string: str, shared_secret: str) -> str:
        """Mirrors: EncryptionUtils.decryptJwe(jweString, sharedSecret)"""
        key = cls._sha256_oct_key(shared_secret)
        token = jwe.JWE()
        token.deserialize(jwe_string, key=key)
        return token.payload.decode("utf-8")

    # ── Symmetric JWS (HS256) ────────────────────────────────────────────

    @classmethod
    def create_jws_symmetric(
        cls,
        payload: str,
        shared_secret: str,
        extra_headers: Optional[dict[str, Any]] = None,
    ) -> str:
        """Mirrors: EncryptionUtils.createJws(jwe, sharedSecret, headers)"""
        protected_header: dict[str, Any] = {
            "alg": "HS256",
            "typ": "JOSE",
            "cty": "JWE",
        }
        if extra_headers:
            protected_header.update(extra_headers)

        key = cls._raw_oct_key(shared_secret)
        token = jws.JWS(payload.encode("utf-8"))
        token.add_signature(key, protected=json_encode(protected_header))
        return token.serialize(compact=True)

    @classmethod
    def verify_and_extract_jws_symmetric(
        cls, jws_string: str, shared_secret: str
    ) -> str:
        """Mirrors: EncryptionUtils.verifyAndExtractJweFromJWS(jws, sharedSecret)"""
        key = cls._raw_oct_key(shared_secret)
        token = jws.JWS()
        token.deserialize(jws_string)
        token.verify(key)
        return token.payload.decode("utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# CHUB API Client — mTLS + API-key auth + JWE/JWS
# ─────────────────────────────────────────────────────────────────────────────

class CHUBClient:
    """
    Axis CHUB gateway client.

    Handles the complete lifecycle:
      1. mTLS handshake (client .p12 keystore)
      2. JWE encryption of request payload (RSA-OAEP-256 + A256GCM)
      3. JWS signing of the JWE token (RS256)
      4. API call with X-IBM-Client-Id/Secret + x-fapi-* headers
      5. JWS verification + JWE decryption of response
    """

    def __init__(self, config: CHUBConfig):
        self.config = config
        self.crypto = CryptoUtils()
        self._axis_pub_key: Optional[jwk.JWK] = None
        self._client_priv_key: Optional[jwk.JWK] = None

        self._client_cert_pem: Optional[bytes] = None
        self._client_key_pem: Optional[bytes] = None
        self._ca_pems: list[bytes] = []

        self._load_keys()

    def _load_keys(self) -> None:
        cert_path = self.config.axis_encryption_cert_path
        if cert_path.exists():
            self._axis_pub_key = self.crypto.load_public_key_from_cert(cert_path)
            logger.info("Loaded Axis encryption cert: %s", cert_path.name)

        p12_path = self.config.p12_path
        if p12_path and p12_path.exists():
            self._load_from_p12(p12_path)
        else:
            self._load_from_pem_files()

    def _load_from_p12(self, p12_path: Path) -> None:
        p12_data = p12_path.read_bytes()
        password = (
            self.config.p12_password.encode("utf-8")
            if self.config.p12_password
            else None
        )
        private_key, certificate, ca_certs = pkcs12.load_key_and_certificates(
            p12_data, password
        )

        if private_key:
            self._client_key_pem = private_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
            self._client_priv_key = jwk.JWK.from_pem(self._client_key_pem)

        if certificate:
            self._client_cert_pem = certificate.public_bytes(serialization.Encoding.PEM)

        if ca_certs:
            self._ca_pems = [
                c.public_bytes(serialization.Encoding.PEM) for c in ca_certs
            ]

        logger.info(
            "Loaded P12 keystore: %s (key=%s, cert=%s, ca_certs=%d)",
            p12_path.name,
            "yes" if private_key else "no",
            "yes" if certificate else "no",
            len(ca_certs or []),
        )

    def _load_from_pem_files(self) -> None:
        key_path = self.config.client_key_path
        if key_path and key_path.exists():
            self._client_key_pem = key_path.read_bytes()
            self._client_priv_key = self.crypto.load_private_key(key_path)
            logger.info("Loaded client private key: %s", key_path.name)

        cert_path = self.config.client_cert_path
        if cert_path and cert_path.exists():
            self._client_cert_pem = cert_path.read_bytes()

        for ca_path in (self.config.root_ca_path, self.config.intermediate_ca_path):
            if ca_path and ca_path.exists():
                self._ca_pems.append(ca_path.read_bytes())

    def _build_ssl_context(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context()

        if self._ca_pems:
            ca_data = b"\n".join(self._ca_pems).decode("ascii")
            ctx.load_verify_locations(cadata=ca_data)

        if self._client_cert_pem and self._client_key_pem:
            cert_tmp = key_tmp = None
            try:
                with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as f:
                    f.write(self._client_cert_pem)
                    cert_tmp = f.name
                with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as f:
                    f.write(self._client_key_pem)
                    key_tmp = f.name
                ctx.load_cert_chain(certfile=cert_tmp, keyfile=key_tmp)
                logger.info("mTLS configured with client cert + key")
            finally:
                if cert_tmp:
                    os.unlink(cert_tmp)
                if key_tmp:
                    os.unlink(key_tmp)

        return ctx

    def encrypt_request(self, payload: dict[str, Any]) -> str:
        """plaintext dict → JWS(JWE(json))"""
        plaintext = json.dumps(payload, separators=(",", ":"))

        if self.config.mode == "asymmetric":
            if not self._axis_pub_key:
                raise ValueError("Axis encryption cert not loaded")
            if not self._client_priv_key:
                raise ValueError("Client private key not loaded")

            jwe_token = self.crypto.create_jwe_rsa(plaintext, self._axis_pub_key)
            jws_token = self.crypto.create_jws_rsa(jwe_token, self._client_priv_key)
        else:
            jwe_token = self.crypto.create_jwe_symmetric(
                plaintext, self.config.client_id, self.config.client_secret,
            )
            jws_token = self.crypto.create_jws_symmetric(
                jwe_token, self.config.client_secret,
            )

        logger.debug(
            "Encrypted: %d bytes plaintext → %d bytes JWE → %d bytes JWS",
            len(plaintext), len(jwe_token), len(jws_token),
        )
        return jws_token

    def decrypt_response(self, raw_body: str) -> dict[str, Any]:
        """JWS(JWE(json)) → plaintext dict"""
        if self.config.mode == "asymmetric":
            if not self._axis_pub_key:
                raise ValueError("Axis public key not loaded for JWS verification")
            if not self._client_priv_key:
                raise ValueError("Client private key not loaded for JWE decryption")

            jwe_token = self.crypto.verify_and_extract_jws_rsa(
                raw_body, self._axis_pub_key
            )
            plaintext = self.crypto.decrypt_jwe_rsa(jwe_token, self._client_priv_key)
        else:
            jwe_token = self.crypto.verify_and_extract_jws_symmetric(
                raw_body, self.config.client_secret
            )
            plaintext = self.crypto.decrypt_jwe_symmetric(
                jwe_token, self.config.client_secret
            )

        return json.loads(plaintext)

    def call_api(
        self,
        path: str,
        payload: dict[str, Any],
        method: str = "POST",
        extra_headers: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        """Full request lifecycle: encrypt → send → decrypt."""
        encrypted_body = self.encrypt_request(payload)
        request_uuid = uuid.uuid4().hex

        ssl_ctx = self._build_ssl_context()
        with httpx.Client(
            base_url=self.config.base_url, verify=ssl_ctx, timeout=30.0
        ) as client:
            headers = {
                "Content-Type": "application/jose",
                "X-IBM-Client-Id": self.config.client_id,
                "X-IBM-Client-Secret": self.config.client_secret,
                "x-fapi-channel-id": self.config.channel_id,
                "x-fapi-epoch-millis": str(int(time.time() * 1000)),
                "x-fapi-uuid": request_uuid,
            }
            if extra_headers:
                headers.update(extra_headers)

            logger.info("%s %s%s (uuid=%s)", method, self.config.base_url, path, request_uuid)
            resp = client.request(method, path, content=encrypted_body, headers=headers)

            logger.info("Response: %d %s (%d bytes)", resp.status_code, resp.reason_phrase, len(resp.content))

            if resp.status_code != 200:
                logger.error("Error body: %.500s", resp.text)
                resp.raise_for_status()

            return self.decrypt_response(resp.text)


# ─────────────────────────────────────────────────────────────────────────────
# Local Self-Test
# ─────────────────────────────────────────────────────────────────────────────

def _divider(title: str) -> None:
    print(f"\n{'─' * 72}")
    print(f"  {title}")
    print(f"{'─' * 72}")


def run_self_test() -> None:
    """Verify the complete encrypt → sign → verify → decrypt pipeline locally."""
    print("=" * 72)
    print("  CHUB Crypto Self-Test  (local round-trip, no network)")
    print("=" * 72)

    crypto = CryptoUtils()
    test_payload = {
        "accountNumber": "1234567890",
        "mobileNumber": "9876543210",
        "action": "test",
    }
    plaintext = json.dumps(test_payload, separators=(",", ":"))

    # ── 1. Asymmetric round-trip ─────────────────────────────────────────
    _divider("1. Asymmetric mode  (RSA-OAEP-256 / A256GCM / RS256)")

    priv_key, pub_key = crypto.generate_test_keypair()

    jwe_tok = crypto.create_jwe_rsa(plaintext, pub_key)
    parts = jwe_tok.split(".")
    assert len(parts) == 5, f"Expected 5 JWE parts, got {len(parts)}"
    print(f"  JWE created  : {len(jwe_tok):,} bytes, {len(parts)} parts")

    jws_tok = crypto.create_jws_rsa(jwe_tok, priv_key)
    parts = jws_tok.split(".")
    assert len(parts) == 3, f"Expected 3 JWS parts, got {len(parts)}"
    print(f"  JWS created  : {len(jws_tok):,} bytes, {len(parts)} parts")

    extracted_jwe = crypto.verify_and_extract_jws_rsa(jws_tok, pub_key)
    assert extracted_jwe == jwe_tok
    print("  JWS verified : signature valid, JWE extracted")

    decrypted = crypto.decrypt_jwe_rsa(extracted_jwe, priv_key)
    assert decrypted == plaintext
    print(f"  JWE decrypted: {decrypted}")
    print("  RESULT: PASSED")

    # ── 2. Symmetric round-trip ──────────────────────────────────────────
    _divider("2. Symmetric mode  (A256GCMKW / A256GCM / HS256)")

    api_key = CLIENT_ID
    secret = CLIENT_SECRET

    jwe_tok = crypto.create_jwe_symmetric(plaintext, api_key, secret)
    parts = jwe_tok.split(".")
    assert len(parts) == 5
    print(f"  JWE created  : {len(jwe_tok):,} bytes, {len(parts)} parts")

    jws_tok = crypto.create_jws_symmetric(jwe_tok, secret)
    parts = jws_tok.split(".")
    assert len(parts) == 3
    print(f"  JWS created  : {len(jws_tok):,} bytes, {len(parts)} parts")

    extracted_jwe = crypto.verify_and_extract_jws_symmetric(jws_tok, secret)
    assert extracted_jwe == jwe_tok
    print("  JWS verified : HMAC valid, JWE extracted")

    decrypted = crypto.decrypt_jwe_symmetric(extracted_jwe, secret)
    assert decrypted == plaintext
    print(f"  JWE decrypted: {decrypted}")
    print("  RESULT: PASSED")

    # ── 3. Axis UAT encryption cert ──────────────────────────────────────
    _divider("3. Axis UAT encryption certificate")

    if AXIS_ENCRYPTION_CERT_PATH.exists():
        axis_key = crypto.load_public_key_from_cert(AXIS_ENCRYPTION_CERT_PATH)

        cert_obj = x509.load_pem_x509_certificate(
            AXIS_ENCRYPTION_CERT_PATH.read_bytes()
        )
        print(f"  Subject : {cert_obj.subject.rfc4514_string()}")
        print(f"  Issuer  : {cert_obj.issuer.rfc4514_string()}")
        print(f"  Valid   : {cert_obj.not_valid_before} → {cert_obj.not_valid_after}")

        jwe_with_axis = crypto.create_jwe_rsa(plaintext, axis_key)
        print(f"  JWE with Axis cert: {len(jwe_with_axis):,} bytes")
        print("  (decryption impossible locally — only Axis holds the private key)")
        print("  RESULT: PASSED")
    else:
        print(f"  SKIPPED: cert not found at {AXIS_ENCRYPTION_CERT_PATH}")

    # ── 4. Client mTLS certificate ───────────────────────────────────────
    _divider("4. Client mTLS certificate (SARVAM)")

    if CLIENT_CERT_PATH.exists():
        cert_obj = x509.load_pem_x509_certificate(CLIENT_CERT_PATH.read_bytes())
        print(f"  Subject : {cert_obj.subject.rfc4514_string()}")
        print(f"  Issuer  : {cert_obj.issuer.rfc4514_string()}")
        print(f"  Valid   : {cert_obj.not_valid_before} → {cert_obj.not_valid_after}")
        print("  RESULT: PASSED")
    else:
        print(f"  SKIPPED: cert not found at {CLIENT_CERT_PATH}")

    print(f"\n{'=' * 72}")
    print("  ALL SELF-TESTS PASSED")
    print(f"{'=' * 72}\n")


# ─────────────────────────────────────────────────────────────────────────────
# API Test Runner
# ─────────────────────────────────────────────────────────────────────────────

def run_api_test(config: CHUBConfig, endpoint: str, payload: dict[str, Any]) -> None:
    print("=" * 72)
    print(f"  CHUB API Test — {config.mode} mode")
    print(f"  Target: {config.base_url}{endpoint}")
    print(f"  Channel: {config.channel_id}")
    print("=" * 72)

    client = CHUBClient(config)

    print(f"\nRequest payload:\n{json.dumps(payload, indent=2)}")

    encrypted = client.encrypt_request(payload)
    header_b64 = encrypted.split(".")[0]
    header_json = json.loads(
        base64.urlsafe_b64decode(header_b64 + "==").decode("utf-8")
    )
    print(f"\nJWS token ({len(encrypted):,} bytes)")
    print(f"  Header: {json.dumps(header_json, indent=2)}")

    try:
        response = client.call_api(endpoint, payload)
        print(f"\nDecrypted response:\n{json.dumps(response, indent=2)}")
    except httpx.HTTPStatusError as exc:
        print(f"\nHTTP {exc.response.status_code}: {exc.response.text[:1000]}")
    except Exception as exc:
        print(f"\nError ({type(exc).__name__}): {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Axis Bank CHUB Integration Test Client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Crypto self-test (no network, no private key)
  python chub_test.py --self-test

  # Send message via CHUB (default endpoint + sarvam.p12)
  python chub_test.py

  # Custom endpoint
  python chub_test.py --endpoint /axis/uat-nondmz/api/v1/partnerai/chub/outbound/send-message

  # Custom payload
  python chub_test.py --payload '{"Data":{...},"Risk":{}}'
""",
    )

    parser.add_argument(
        "--self-test", action="store_true",
        help="Run local crypto round-trip test (no network needed)",
    )
    parser.add_argument(
        "--endpoint",
        help=f"API endpoint path (default: {DEFAULT_ENDPOINT})",
    )
    parser.add_argument("--payload", help="JSON request body (default: CHUB test message)")
    parser.add_argument(
        "--p12", type=Path,
        help="Path to PKCS12 keystore (default: certs/sarvam.p12)",
    )
    parser.add_argument("--p12-password", help="PKCS12 export password (default: empty)")
    parser.add_argument(
        "--private-key", type=Path,
        help="Path to RSA private key PEM (legacy, use --p12 instead)",
    )
    parser.add_argument(
        "--client-cert", type=Path,
        help="Override client certificate path (legacy, use --p12 instead)",
    )
    parser.add_argument(
        "--mode", choices=["asymmetric", "symmetric"], default="asymmetric",
        help="Encryption mode (default: asymmetric)",
    )
    parser.add_argument("--base-url", help="Override UAT base URL")
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.self_test:
        run_self_test()
        return

    config = CHUBConfig(mode=args.mode)
    if args.p12:
        config.p12_path = args.p12
    if args.p12_password is not None:
        config.p12_password = args.p12_password
    if args.private_key:
        config.p12_path = None  # disable .p12, use legacy PEM files
        config.client_key_path = args.private_key
    if args.client_cert:
        config.client_cert_path = args.client_cert
    if args.base_url:
        config.base_url = args.base_url

    endpoint = args.endpoint or config.default_endpoint

    now_str = time.strftime("%d-%b-%YT%H:%M:%S.000").upper()
    trans_id = f"SARVAM{int(time.time())}"
    payload = (
        json.loads(args.payload)
        if args.payload
        else {
            "Data": {
                "appId": "1461",
                "apiKey": "partnerAI1461",
                "logInfo": {
                    "system": "partnerAI",
                    "msgGenTime": now_str,
                    "msgPutTime": now_str,
                },
                "context": {
                    "transId": trans_id,
                    "templateId": "partnerAI0010",
                    "senderId": "",
                },
                "collection": [
                    {
                        "data": {
                            "toEmailId": "",
                            "ccEmailId": "",
                            "bccEmailId": "",
                            "phoneNo": "919876543210",
                            "altPhoneNo": "",
                            "altEmail": "",
                            "subject": "",
                            "smsg": "",
                            "custom": {
                                "CHANNEL": "W",
                                "EVENT_NAME": "",
                                "FREE_TEXT_1": "Hello from Sarvam AI",
                                "SRC_SYS_NAME": "partner.Ai",
                                "FREQUENCY": "REAL TIME",
                                "DIVISION_ID": "partnerAI_CFCUG",
                                "SUB_DIVISION_ID": "",
                                "PRIORITY": "2",
                                "INTR_FLAG": "1",
                            },
                        }
                    }
                ],
            },
            "Risk": {},
        }
    )

    run_api_test(config, endpoint, payload)


if __name__ == "__main__":
    main()
