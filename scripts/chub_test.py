#!/usr/bin/env python3
"""
Axis CHUB Send Message — minimal test client.

  plaintext → JWE(RSA-OAEP-256, A256GCM) → JWS(RS256) → HTTPS+mTLS → CHUB
"""
from __future__ import annotations

import base64
import json
import logging
import os
import ssl
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12
from jwcrypto import jwe, jwk, jws
from jwcrypto.common import json_encode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("chub")

# ── Config ───────────────────────────────────────────────────────────────────

CERTS_DIR = Path("/home/sarvam/axis/certs")

AXIS_ENCRYPT_CERT = CERTS_DIR / "rgw.jwejws.uat.axisb.com-sscert.txt"
P12_FILE = CERTS_DIR / "sarvam.p12"
P12_PASSWORD = ""

BASE_URL = "https://sakshamuat.axis.bank.in"
ENDPOINT = "/gateway/api/v1/collections/chub/outbound/send-message"

CLIENT_ID = "1f42ebc532ab4b08f19d75209e0ceca1"
CLIENT_SECRET = "6f04441c1b790d40bb2f41fb4c616bd1"
CHANNEL_ID = "SARVAM"

APP_ID = "1511"
API_KEY = "SARVAM1511"
SYSTEM_NAME = "SARVAM"
TEMPLATE_ID = "KOREAI0011"
DIVISION_ID = "Sarvam_CFCUG"
SENDER_ID = "1107900001216700002"


# ── Crypto ───────────────────────────────────────────────────────────────────

def load_pub_key_from_cert(path: Path) -> jwk.JWK:
    cert = x509.load_pem_x509_certificate(path.read_bytes())
    pub_pem = cert.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return jwk.JWK.from_pem(pub_pem)


def load_p12(path: Path, password: str = "") -> tuple[jwk.JWK, bytes, bytes, list[bytes]]:
    """Returns (private_jwk, cert_pem, key_pem, ca_pems)."""
    pw = password.encode() if password else None
    priv_key, cert, ca_certs = pkcs12.load_key_and_certificates(path.read_bytes(), pw)

    key_pem = priv_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    ca_pems = [c.public_bytes(serialization.Encoding.PEM) for c in (ca_certs or [])]

    return jwk.JWK.from_pem(key_pem), cert_pem, key_pem, ca_pems


def encrypt(plaintext: str, axis_pub: jwk.JWK, client_priv: jwk.JWK) -> str:
    """plaintext → JWE → JWS compact token."""
    jwe_tok = jwe.JWE(
        plaintext.encode(),
        recipient=axis_pub,
        protected=json_encode({"alg": "RSA-OAEP-256", "enc": "A256GCM"}),
    )
    jwe_compact = jwe_tok.serialize(compact=True)

    jws_tok = jws.JWS(jwe_compact.encode())
    jws_tok.add_signature(client_priv, protected=json_encode({"alg": "RS256"}))
    return jws_tok.serialize(compact=True)


def decrypt(body: str, axis_pub: jwk.JWK, client_priv: jwk.JWK) -> dict:
    """JWS compact token → JWE → plaintext dict."""
    jws_tok = jws.JWS()
    jws_tok.deserialize(body)
    jws_tok.verify(axis_pub)
    jwe_compact = jws_tok.payload.decode()

    jwe_tok = jwe.JWE()
    jwe_tok.deserialize(jwe_compact, key=client_priv)
    return json.loads(jwe_tok.payload.decode())


# ── mTLS ─────────────────────────────────────────────────────────────────────

def build_ssl_ctx(cert_pem: bytes, key_pem: bytes, ca_pems: list[bytes]) -> ssl.SSLContext:
    ctx = ssl.create_default_context()

    if ca_pems:
        ctx.load_verify_locations(cadata=b"\n".join(ca_pems).decode())

    cert_tmp = key_tmp = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as f:
            f.write(cert_pem); cert_tmp = f.name
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as f:
            f.write(key_pem); key_tmp = f.name
        ctx.load_cert_chain(certfile=cert_tmp, keyfile=key_tmp)
    finally:
        if cert_tmp: os.unlink(cert_tmp)
        if key_tmp: os.unlink(key_tmp)

    return ctx


# ── Payload ──────────────────────────────────────────────────────────────────

def build_payload(phone: str) -> dict[str, Any]:
    now = time.strftime("%d-%b-%YT%H:%M:%S.000").upper()
    return {
        "Data": {
            "appId": APP_ID,
            "apiKey": API_KEY,
            "logInfo": {
                "system": SYSTEM_NAME,
                "msgGenTime": now,
                "msgPutTime": now,
            },
            "context": {
                "transId": f"{APP_ID}{int(time.time())}",
                "templateId": TEMPLATE_ID,
                "senderId": SENDER_ID,
            },
            "collection": [{
                "data": {
                    "toEmailId": "",
                    "ccEmailId": "",
                    "bccEmailId": "",
                    "phoneNo": phone,
                    "altPhoneNo": "",
                    "altEmail": "",
                    "subject": "",
                    "smsg": "",
                    "custom": {
                        "CHANNEL": "S",
                        "EVENT_NAME": "PTP disposition for Pre Delinquent customer for Loans",
                        "custFirstName": "Divya",
                        "EMI_amt": "10000",
                        "Product_Name": "PL",
                        "Account_number": "8668",
                        "DATE": time.strftime("%d-%m-%y"),
                        "total_outstanding": "100000",
                        "min_due_amt": "100",
                        "overdue_amt": "10000",
                        "charges": "2000",
                        "End_date": time.strftime("%d-%m-%y"),
                        "link": "",
                        "link_1": "aHR0cHM6Ly93d3cuYXhpcy5iYW5rLmluL3BheWVtaQ==",
                        "FREE_TEXT_1": "",
                        "FREE_TEXT_2": "",
                        "FREE_TEXT_3": "",
                        "FREE_TEXT_4": "",
                        "FREE_TEXT_5": "",
                        "FREE_TEXT_6": "",
                        "FREE_TEXT_7": "",
                        "FREE_TEXT_8": "",
                        "FREE_TEXT_9": "",
                        "FREE_TEXT_10": "",
                        "FREE_TEXT_11": "",
                        "FREE_TEXT_12": "",
                        "FREE_TEXT_13": "",
                        "FREE_TEXT_14": "",
                        "FREE_TEXT_15": "",
                        "FREE_TEXT_16": "",
                        "FREE_TEXT_17": "",
                        "FREE_TEXT_18": "",
                        "FREE_TEXT_19": "",
                        "FREE_TEXT_20": "",
                        "FREE_TEXT_21": "",
                        "FREE_TEXT_22": "",
                        "FREE_TEXT_23": "",
                        "FREE_TEXT_24": "",
                        "FREE_TEXT_25": "",
                        "SCHM_TYPE": "",
                        "SCHM_CODE": "",
                        "SCHM_DESC": "",
                        "SRC_SYS_NAME": SYSTEM_NAME,
                        "FREQUENCY": "REAL TIME",
                        "DIVISION_ID": DIVISION_ID,
                        "SUB_DIVISION_ID": "",
                        "PRIORITY": "2",
                        "INTR_FLAG": "1",
                    },
                }
            }],
        },
        "Risk": {},
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    phone = sys.argv[1] if len(sys.argv) > 1 else "9407929920"
    payload_override = sys.argv[2] if len(sys.argv) > 2 else None

    # Load keys
    axis_pub = load_pub_key_from_cert(AXIS_ENCRYPT_CERT)
    log.info("Loaded Axis encryption cert")

    client_priv, cert_pem, key_pem, ca_pems = load_p12(P12_FILE, P12_PASSWORD)
    log.info("Loaded P12 keystore: %s", P12_FILE.name)

    # Build payload
    payload = json.loads(payload_override) if payload_override else build_payload(phone)
    print(f"\nPayload:\n{json.dumps(payload, indent=2)}\n")

    # Encrypt
    body = encrypt(json.dumps(payload, separators=(",", ":")), axis_pub, client_priv)
    log.info("Encrypted: %d bytes JWS", len(body))

    # Send
    request_id = uuid.uuid4().hex
    ssl_ctx = build_ssl_ctx(cert_pem, key_pem, ca_pems)
    log.info("mTLS configured")

    with httpx.Client(base_url=BASE_URL, verify=ssl_ctx, timeout=30.0) as client:
        headers = {
            "Content-Type": "application/jose",
            "X-IBM-Client-Id": CLIENT_ID,
            "X-IBM-Client-Secret": CLIENT_SECRET,
            "x-fapi-channel-id": CHANNEL_ID,
            "x-fapi-epoch-millis": str(int(time.time() * 1000)),
            "x-fapi-uuid": request_id,
            "x-fapi-serviceId": "openapi",
            "x-fapi-serviceVersion": "1.0",
        }

        log.info("POST %s%s (uuid=%s)", BASE_URL, ENDPOINT, request_id)
        resp = client.post(ENDPOINT, content=body, headers=headers)
        log.info("Response: %d %s (%d bytes)", resp.status_code, resp.reason_phrase, len(resp.content))

        if resp.status_code != 200:
            print(f"\nERROR {resp.status_code}: {resp.text}")
            sys.exit(1)

        result = decrypt(resp.text, axis_pub, client_priv)
        print(f"\nResponse:\n{json.dumps(result, indent=2)}")


if __name__ == "__main__":
    main()
