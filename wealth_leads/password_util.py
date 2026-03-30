"""PBKDF2 password hashes — no extra deps."""
from __future__ import annotations

import hashlib
import hmac
import secrets


def hash_password(plain: str, iterations: int = 310_000) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        plain.encode("utf-8"),
        salt.encode("ascii"),
        iterations,
    )
    return f"pbkdf2_sha256${iterations}${salt}${dk.hex()}"


def verify_password(plain: str, stored: str) -> bool:
    if not stored or "$" not in stored:
        return False
    parts = stored.split("$")
    if len(parts) != 4 or parts[0] != "pbkdf2_sha256":
        return False
    _, it_s, salt, hx = parts
    try:
        iterations = int(it_s)
    except ValueError:
        return False
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        plain.encode("utf-8"),
        salt.encode("ascii"),
        iterations,
    )
    return hmac.compare_digest(dk.hex(), hx)
