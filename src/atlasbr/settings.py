"""
AtlasBR - Global Settings & Logging.
"""
import os
import logging
from pathlib import Path
from typing import Optional

# Define a library-specific logger
logger = logging.getLogger("atlasbr")
logger.addHandler(logging.NullHandler()) # Default to silence unless configured

class Settings:
    _instance = None
    
    def __init__(self):
        self.gcp_billing_id: Optional[str] = os.getenv("GCLOUD_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
        
        # Default cache location
        env_cache = os.getenv("ATLASBR_CACHE_DIR")
        if env_cache:
            self.cache_dir = Path(env_cache)
        else:
            self.cache_dir = Path.cwd() / ".atlasbr_cache"

    @classmethod
    def _get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def get_billing_id(cls) -> str:
        inst = cls._get_instance()
        if inst.gcp_billing_id is None:
            raise ValueError(
                "GCP Billing ID not set. "
                "Set 'GCLOUD_PROJECT_ID' env var or call 'atlasbr.set_billing_id()'."
            )
        return inst.gcp_billing_id

    @classmethod
    def set_billing_id(cls, project_id: str):
        inst = cls._get_instance()
        inst.gcp_billing_id = project_id

    @classmethod
    def get_cache_dir(cls) -> Path:
        inst = cls._get_instance()
        # Ensure dir exists when requested
        inst.cache_dir.mkdir(parents=True, exist_ok=True)
        return inst.cache_dir

# --- Public Helpers (Exposed in __init__.py) ---

def get_billing_id() -> str:
    """Retrieves the current billing ID."""
    return Settings.get_billing_id()

def set_billing_id(project_id: str):
    """Sets the Google Cloud Project ID for all subsequent calls."""
    Settings.set_billing_id(project_id)

def get_cache_dir() -> Path:
    """Retrieves the current cache directory path."""
    return Settings.get_cache_dir()

def configure_logging(level: int = logging.INFO):
    """Helper to enable console logging for the library."""
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)