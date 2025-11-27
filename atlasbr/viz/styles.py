"""
AtlasBR - Visualization Styles & Theme Constants.
"""
from typing import Dict, Any

SLIDER_STYLE = dict(
    len=0.25,
    x=0.75,
    xanchor="center",
    y=1,
    yanchor="bottom",
    pad={"t": 20, "b": 10},
)
DROPDOWN_STYLE = dict(
    x=0.01,
    xanchor="left",
    y=1.0,
    yanchor="bottom",
    pad={"r": 6, "t": 5},
)
COLORBAR_STYLE = dict(thickness=12, len=0.75)

def build_coloraxis(spec: Dict[str, Any]) -> Dict:
    """Builds a coloraxis dictionary from a variable-year specification."""
    title_suffix = " (log10)" if spec.get("log_color") else ""
    colorbar = {**COLORBAR_STYLE, "title": spec["title"] + title_suffix}

    if spec["type"] == "discrete":
        colorbar.update(
            tickmode="array",
            tickvals=list(range(spec["k"])),
            ticktext=spec["labels"],
        )
        return dict(
            cmin=-0.5,
            cmax=spec["k"] - 0.5,
            colorscale=spec["colorscale"],
            colorbar=colorbar,
        )
    else:  # continuous
        return dict(
            cmin=spec["cmin"],
            cmax=spec["cmax"],
            colorscale=spec["colorscale"],
            colorbar=colorbar,
        )