import sys
import pytest
from unittest.mock import patch

def test_atlasbr_imports_without_optional_deps():
    """
    Ensure core atlasbr modules import even if 'basedosdados', 'plotly', etc. are missing.
    """
    # List of modules to simulate as missing
    missing_modules = ["basedosdados", "plotly", "mapclassify", "tobler", "h3", "geobr"]
    
    with patch.dict(sys.modules, {m: None for m in missing_modules}):
        # Force reload of atlasbr to test import logic in a clean environment
        # Note: We iterate over keys to avoid 'dictionary changed size during iteration'
        keys_to_remove = [k for k in sys.modules if k.startswith("atlasbr")]
        for k in keys_to_remove:
            del sys.modules[k]
            
        import atlasbr
        assert atlasbr is not None
        
        # Check specific app submodules that shouldn't crash
        import atlasbr.app.census
        import atlasbr.app.rais
        assert atlasbr.app.census is not None

def test_lazy_adapter_imports():
    """
    Verify that importing app modules does not trigger adapter imports 
    (which would trigger backend deps).
    """
    # Clean sys.modules of atlasbr to ensure fresh import
    keys_to_remove = [k for k in sys.modules if k.startswith("atlasbr")]
    for k in keys_to_remove:
        del sys.modules[k]
            
    import atlasbr.app.census
    import atlasbr.app.rais
    
    # Check that concrete adapters are NOT in sys.modules yet
    # If this fails, it means an 'import atlasbr.infra.adapters.census_bd' leaked 
    # into the top-level of an app module.
    assert "atlasbr.infra.adapters.census_bd" not in sys.modules
    assert "atlasbr.infra.adapters.rais_bd" not in sys.modules