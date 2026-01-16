from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PlotStyle:
    title: Optional[str] = None
    xlabel: Optional[str] = None
    ylabel: Optional[str] = None
    tight: bool = True


@dataclass(frozen=True)
class MapStyle:
    title: Optional[str] = None
    basemap: bool = True
    basemap_source: str = "CartoDB.Positron"
    north_arrow: bool = True
    scale_bar: bool = True
    axis_off: bool = True
