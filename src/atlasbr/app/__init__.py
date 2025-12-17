"""
AtlasBR - Application Layer.

Modules:
- census: Load Census data.
- rais: Load Employment data.
- cnes: Load Healthcare Infrastructure.
- inep: Load Schools.
"""

# Explicitly empty to prevent eager loading.
# Users should use: from atlasbr.app.census import load_census