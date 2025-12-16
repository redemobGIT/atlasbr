"""AtlasBR disk cache utilities."""

from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path
from typing import Optional

import requests

from atlasbr.settings import get_cache_dir, logger


def url_to_filename(url: str, *, suffix: str = "") -> str:
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return f"{h}{suffix}"


def cached_download(
    url: str,
    *,
    relpath: Path,
    timeout: int = 120,
    force: bool = False,
) -> Path:
    """Download a URL to the AtlasBR cache directory (or reuse if present)."""
    cache_dir = get_cache_dir()
    out = cache_dir / relpath
    out.parent.mkdir(parents=True, exist_ok=True)

    if out.exists() and not force:
        return out

    logger.info(f"    ⬇️  Downloading (cached): {url}")
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    out.write_bytes(resp.content)
    return out


def cached_extract_zip(
    zip_path: Path,
    *,
    extract_dir: Path,
    force: bool = False,
) -> Path:
    """Extract a zip archive into extract_dir (or reuse existing extraction)."""
    if extract_dir.exists() and any(extract_dir.rglob("*")) and not force:
        return extract_dir

    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
    return extract_dir


def find_first_file(root: Path, pattern: str) -> Optional[Path]:
    """Return the first match for pattern under root (recursive), if any."""
    for p in sorted(root.rglob(pattern)):
        return p
    return None