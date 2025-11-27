# AtlasBR — A Python Toolkit for Brazilian Spatial Data Pipelines

<table>
  <tr>
    <td width="170" valign="top">
      <img src="brand/atlasbr_logo.png" alt="AtlasBR logo" width="150">
    </td>
    <td valign="center">
      <p>
      AtlasBR is a Python library for the reproducible extraction, harmonization,
      and spatial integration of Brazilian socio-economic data. It provides a
      domain-driven, hexagonal architecture to unify heterogeneous sources
      (Census, RAIS, CNES, Schools) into canonical spatial formats,
      handling the complexities of geometry matching, areal interpolation (H3),
      and public-sector data federation.
      </p>
    </td>
  </tr>
</table>

## Core Features

AtlasBR solves the "fragmentation problem" of Brazilian urban data (Census, Labor, Health, Education) by acting as a universal adapter.

  * **Unified Data Tables:**
    Instead of juggling separate files for private jobs (RAIS), schools (INEP), and hospitals (CNES), AtlasBR consolidates them into a **single harmonized table**. It automatically normalizes column names and classifications (CNAE), filling gaps in public sector coverage seamlessly.

  * **Geospatial by Default:**
    Every function returns a ready-to-map **GeoDataFrame**. The library handles the messy work of assigning geometry behind the scenes—using exact coordinates for schools, postal code centroids for businesses, and official tracts for census data—so you get a spatial object instantly.

  * **Standardized & Comparable (H3):**
    To make disparate spatial units comparable (e.g., comparing Census Tracts to Neighborhoods), AtlasBR offers built-in **H3 grid harmonization**. With one parameter, you can re-aggregate any dataset into a regular hexagonal grid for apples-to-apples comparison.

  * **Simple Command Lines:**
    Complex ETL pipelines (Extract-Transform-Load) are wrapped into high-level functions. You don't write SQL or handle API retries; you just request the data you need for the places you want.

## Installation

1.  Clone the repository:

    ```bash
    git clone https://github.com/your-org/atlasbr.git
    cd atlasbr
    ```

2.  Install in editable mode (recommended for development):

    ```bash
    pip install -e .
    ```

3.  Configure Google Cloud Billing (required for Base dos Dados access):

    ```python
    import atlasbr
    atlasbr.set_billing_id("your-gcp-project-id")
    ```

## Usage Example

Notice how fragmented tasks (fetching geometry, downloading CSVs, cleaning outliers, merging datasets) are collapsed into single commands.

```python
import atlasbr

# 1. Get Census Data (Geometry + Attributes)
# Returns a GeoDataFrame of tracts with income data, clipped to the urban footprint.
df_census = atlasbr.load_census(
    places=["Rio de Janeiro, RJ", "Niterói, RJ"],
    year=2010,
    themes=["income"],
    clip_urban=True
)

# 2. Get Consolidated Employment (Private + Public)
# Unifies RAIS (Private) with INEP (Schools) and CNES (Health) into one geospatial table.
df_jobs = atlasbr.load_rais(
    places=["Rio de Janeiro, RJ"],
    year=2022,
    include_public_sector=True, # <--- The magic unification switch
    geocode=True
)
```