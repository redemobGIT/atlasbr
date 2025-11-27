This `README.md` is designed to live inside your `tutorials/` folder. It bridges the gap between "how do I run this?" and "why is it built this way?", serving as a high-level architectural guide for users and contributors.

-----

# AtlasBR Architecture & Design Guide

Welcome to **AtlasBR**. This library is designed to make Brazilian socio-economic data accessible, reproducible, and easy to integrate into data science workflows.

Unlike simple data-fetching scripts, AtlasBR is built using **Domain-Driven Design (DDD)** and **Hexagonal Architecture** principles. This structure ensures that business logic (e.g., "How do we impute race for children in 2022?") is kept separate from technical details (e.g., "How do we query BigQuery?" or "How do we parse an IBGE ZIP file?").

-----

## 🏗️ The Core Philosophy

We view data acquisition not as a single script, but as a pipeline with three distinct stages:

1.  **Catalog (The "What"):** Defining what data exists and what it looks like.
2.  **Infrastructure (The "How"):** The heavy lifting of fetching data from the outside world.
3.  **Core Logic (The "Math"):** Pure functions that transform raw data into useful metrics.

This separation allows us to swap out data sources (e.g., moving from FTP to BigQuery) without breaking the analysis code.

-----

## 🧩 Architectural Layers

The library is organized into concentric layers, moving from the "outside world" to the "pure domain."

### 1\. `app` (Application Layer)

**Role:** The Orchestrator.

  * **What it does:** This is the entry point for you, the user. Functions like `load_census` or `load_rais` live here. They don't know *how* to calculate things or *how* to fetch data; they just hire the right workers to do it.
  * **Example:** "Get the municipality IDs, ask Infra to fetch the raw Census tables, ask Core to calculate age groups, and return the result."

### 2\. `core` (Domain Layer)

**Role:** The Brain.

  * **What it does:** Contains the "Business Logic" of the library. This code is pure Python, has no network dependencies, and is easily testable.
  * **Sub-modules:**
      * `catalog`: Pydantic models defining the contract for every dataset (e.g., "The 2010 Census Basic theme requires columns `v001` and `v002`").
      * `logic`: Mathematical transformations (e.g., imputing race, harmonizing school metrics, calculating H3 interpolation).

### 3\. `infra` (Infrastructure Layer)

**Role:** The Workers.

  * **What it does:** Handles the "dirty work" of I/O. These adapters talk to the outside world.
  * **Sub-modules:**
      * `adapters`: Connectors for Base dos Dados (`_bd`), IBGE FTP (`_ftp`), etc.
      * `geo`: Fetchers for shapefiles (`geobr` wrappers) and place name resolution.
      * `cache`: Handles disk caching to prevent re-downloading data.

-----

## 🚀 Key Patterns in Use

### The "Federated" Data Model

AtlasBR treats different datasets as "modules" that can be joined together.

  * **RAIS** is the backbone for labor data.
  * **Schools (INEP)** and **Health Units (CNES)** are independent datasets, but the RAIS pipeline can "inject" them to fill the gap of public sector jobs.
  * **Integration Logic:** We use `harmonize_*` functions to translate specific datasets (like Schools) into a common schema (like RAIS) before merging.

### Hybrid Geocoding

We use the best available geometry for the job:

  * **Census:** Uses **Tracts** (Setores Censitários) as the fundamental unit.
  * **Schools:** Uses high-precision **Lat/Lon** coordinates.
  * **RAIS/CNES:** Uses **CEP Centroids** (Postal Codes) when exact coordinates are missing.
  * **H3 Grids:** We support spatial interpolation (using Tobler's areal weighting) to re-aggregate polygon data (Tracts) into hexagonal grids.

-----

## 📂 Directory Structure

```text
src/atlasbr/
├── app/                  # High-level functions (load_census, load_rais)
├── core/
│   ├── catalog/          # Data definitions and specs
│   ├── logic/            # Pure data transformation functions
│   ├── geo/              # Spatial operations (clipping, projection)
│   └── types.py          # Shared type definitions
├── infra/
│   ├── adapters/         # SQL queries and API clients
│   └── geo/              # Downloaders for Shapefiles/Tracts
├── viz/                  # Visualization tools (Plotly maps)
└── settings.py           # Global config (Logging, Secrets)
```

## 🛠️ Contributing

When adding new features, ask yourself:

1.  **Is this a definition?** -\> `core/catalog`
2.  **Is this fetching data?** -\> `infra/adapters`
3.  **Is this transforming data?** -\> `core/logic`
4.  **Is this coordinating steps?** -\> `app`