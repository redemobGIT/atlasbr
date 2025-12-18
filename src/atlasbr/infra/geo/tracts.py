"""
AtlasBR - Infrastructure Geo Adapter (Census Tracts).
"""
import pandas as pd
import geopandas as gpd
import numpy as np
from typing import Iterable, List
from atlasbr.settings import logger


def fetch_tracts_raw(munis: Iterable[int], year: int) -> gpd.GeoDataFrame:
    """
    Fetches raw Census Tracts from geobr for the specified municipalities.
    """
    try:
        import geobr
    except ImportError:
        raise ImportError(
            "The 'geobr' library is required to fetch Census Tracts. "
            "Please install it via `pip install atlasbr[geo]` or "
            "`pip install geobr`."
        )

    muni_list = [int(m) for m in np.atleast_1d(munis)]
    logger.info(
        f"Fetching Census Tracts for {len(muni_list)} "
        f"municipalities (Year {year})..."
    )

    dfs: List[gpd.GeoDataFrame] = []

    for code in muni_list:
        try:
            # verbose=False suppresses geobr's own print statements
            df = geobr.read_census_tract(
                code_tract=code, year=year, simplified=False, verbose=False
            )
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Failed to load tracts for muni {code}: {e}")
            continue

    if not dfs:
        raise RuntimeError(
            "No census tracts found for the requested municipalities."
        )

    return pd.concat(dfs, ignore_index=True)