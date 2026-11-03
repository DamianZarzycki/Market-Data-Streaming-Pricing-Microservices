import os


def env(name: str) -> str:
    """Return the stripped value of `name`. Raise if it is missing or blank."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        raise RuntimeError(f"Missing environment variable {name}")
    return raw.strip()
