from __future__ import annotations
import logging, os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv(override=False)

def get_logger(name: str = "urbikit", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s | %(levelname)s | %(name)s: %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger

def env(var: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(var, default)

def cache_path(key: str) -> Path:
    base = Path(env("URBIKIT_CACHE_DIR", "~/.cache/urbikit")).expanduser()
    base.mkdir(parents=True, exist_ok=True)
    return base / key
