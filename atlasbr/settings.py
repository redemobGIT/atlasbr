"""
AtlasBR - Global Settings.

Manages library-wide configuration defaults (e.g., Google Cloud Project ID).
"""
import os
from typing import Optional

class Settings:
    _instance = None
    
    def __init__(self):
        # Try to load from environment variable by default
        self.gcp_billing_id: Optional[str] = os.getenv("GCLOUD_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")

    @classmethod
    def get_billing_id(cls) -> str:
        if cls._instance is None:
            cls._instance = cls()
            
        if cls._instance.gcp_billing_id is None:
            # Fallback or warning
            raise ValueError(
                "GCP Billing ID is not set. "
                "Set 'GCLOUD_PROJECT_ID' env var or call 'atlasbr.set_billing_id()'."
            )
        return cls._instance.gcp_billing_id

    @classmethod
    def set_billing_id(cls, project_id: str):
        if cls._instance is None:
            cls._instance = cls()
        cls._instance.gcp_billing_id = project_id

# Public Helper
def set_billing_id(project_id: str):
    """Sets the Google Cloud Project ID for all subsequent calls."""
    Settings.set_billing_id(project_id)

def get_billing_id() -> str:
    """Retrieves the current billing ID."""
    return Settings.get_billing_id()