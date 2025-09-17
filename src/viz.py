from __future__ import annotations
from typing import Optional
import geopandas as gpd
import plotly.express as px

def hex_map(df_hex: gpd.GeoDataFrame, value_col: str, *, year_col: Optional[str] = None, title: Optional[str] = None):
    df_hex = df_hex.to_crs(4326)
    geojson = df_hex.__geo_interface__
    kwargs = {"geojson": geojson, "locations": df_hex.index, "color": value_col}
    if year_col:
        kwargs["animation_frame"] = year_col
    fig = px.choropleth_mapbox(df_hex, **kwargs, mapbox_style="carto-positron", zoom=8, center={"lat": -22.9, "lon": -43.2}, opacity=0.8, title=title or value_col)
    fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
    return fig

def cnae_heatmap(df, cnae_col: str, value_col: str):
    return px.density_heatmap(df, x=cnae_col, y=value_col)
