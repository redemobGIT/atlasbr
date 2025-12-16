"""AtlasBR disk cache utilities."""

from __future__ import annotations

import hashlib
import zipfile
import logging
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from atlasbr.settings import get_cache_dir, logger


def url_to_filename(url: str, *, suffix: str = "") -> str:
    """Generates a filesystem-safe filename from a URL using SHA256."""
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return f"{h}{suffix}"


def _get_robust_session(retries: int = 3, backoff_factor: float = 0.5) -> requests.Session:
    """Creates a requests Session with automatic retries and exponential backoff."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(500, 502, 503, 504),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def cached_download(
    url: str,
    *,
    relpath: Path,
    timeout: int = 180,
    force: bool = False,
) -> Path:
    """
    Download a URL to the AtlasBR cache directory (or reuse if present).
    
    Features:
      - Automatic Retries (Network resilience).
      - Streaming Download (Memory efficiency).
      - Atomic Writes (Prevents corrupted partial downloads).
    """
    cache_dir = get_cache_dir()
    out = cache_dir / relpath
    out.parent.mkdir(parents=True, exist_ok=True)

    if out.exists() and not force:
        # Optional: Add file size check or header check here if stricter validity needed
        return out

    logger.info(f"    ⬇️  Downloading (cached): {url}")
    
    session = _get_robust_session()
    temp_out = out.with_suffix(".tmp")
    
    try:
        with session.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            with open(temp_out, "wb") as f:
                # Stream in chunks (8KB) to avoid loading large zips into RAM
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        
        # Rename only on success to ensure atomicity
        if temp_out.exists():
            temp_out.rename(out)
            
    except Exception as e:
        if temp_out.exists():
            temp_out.unlink()
        raise RuntimeError(f"Failed to download {url} after retries.") from e

    return out


def cached_extract_zip(
    zip_path: Path,
    *,
    extract_dir: Path,
    force: bool = False,
) -> Path:
    """
    Extract a zip archive into extract_dir (or reuse existing extraction).
    
    Security:
      - Prevents 'Zip Slip' (path traversal attacks) by validating member paths.
    """
    if extract_dir.exists() and any(extract_dir.iterdir()) and not force:
        return extract_dir

    extract_dir.mkdir(parents=True, exist_ok=True)
    destination = extract_dir.resolve()
    
    logger.info(f"    📦 Extracting: {zip_path.name}")

    with zipfile.ZipFile(zip_path) as zf:
        # Security Check: Ensure all members extract INSIDE the target directory
        for member in zf.namelist():
            member_path = (destination / member).resolve()
            if not member_path.is_relative_to(destination):
                raise ValueError(f"Security violation: Zip Slip detected in {member}")
        
        zf.extractall(extract_dir)
        
    return extract_dir


def find_first_file(root: Path, pattern: str) -> Optional[Path]:
    """Return the first match for pattern under root (recursive), if any."""
    try:
        return next(root.rglob(pattern))
    except StopIteration:
        return None