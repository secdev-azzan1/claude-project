import os


def tls_verify_enabled() -> bool:
    """Return whether outbound HTTPS certificate verification should be enabled.

    Backward-compatible default is insecure TLS disabled verification.
    Set STRICT_TLS_VERIFY=true to enforce certificate validation.
    ALLOW_INSECURE_TLS=false also enforces verification.
    """
    strict = (os.environ.get("STRICT_TLS_VERIFY") or "").strip().lower() in {"1", "true", "yes", "on"}
    if strict:
        return True
    allow_insecure = (os.environ.get("ALLOW_INSECURE_TLS") or "true").strip().lower()
    insecure = allow_insecure in {"1", "true", "yes", "on"}
    return not insecure
