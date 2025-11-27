"""
AtlasBR - Global Settings & Logging.
"""
import os
import logging
from typing import Optional

# Define a library-specific logger
logger = logging.getLogger("atlasbr")
logger.addHandler(logging.NullHandler()) # Default to silence unless configured

class Settings:
    _instance = None
    
    def __init__(self):
        self.gcp_billing_id: Optional[str] = (
            os.getenv("GCLOUD_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
            )

    @classmethod
    def get_billing_id(cls) -> str:
        if cls._instance is None:
            cls._instance = cls()
        if cls._instance.gcp_billing_id is None:
            raise ValueError(
                "GCP Billing ID not set. Export GCLOUD_PROJECT_ID or use settings.set_billing_id()."
                )
        return cls._instance.gcp_billing_id

    @classmethod
    def set_billing_id(cls, project_id: str):
        if cls._instance is None:
            cls._instance = cls()
        cls._instance.gcp_billing_id = project_id

def get_billing_id() -> str:
    return Settings.get_billing_id()

def configure_logging(level: int = logging.INFO):
    """Helper to enable console logging for the library."""
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)