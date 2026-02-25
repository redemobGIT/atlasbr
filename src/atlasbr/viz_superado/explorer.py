import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import geopandas as gpd
from typing import Optional, List, Union, Literal


class SpatialExplorer:
    def __init__(self, gdf: gpd.GeoDataFrame):
        self.gdf = gdf.copy()

    def plot_choropleth(
        self,
        column: str,
        cmap: str = "viridis",
        scheme: Literal["quantiles", "fisherjenks", "equalinterval"] = "quantiles",
        k: int = 5,
        title: Optional[str] = None,
        legend_label: Optional[str] = None,
    ):
        if "geometry" not in self.gdf.columns:
            raise ValueError("O GeoDataFrame precisa da coluna 'geometry' para o mapa.")

        fig, ax = plt.subplots(1, 1, figsize=(12, 10))

        self.gdf.plot(
            column=column,
            cmap=cmap,
            scheme=scheme,
            k=k,
            legend=True,
            legend_kwds={"fmt": "{:.2f}", "bbox_to_anchor": (1, 1)},
            linewidth=0.1,
            edgecolor="black",
            ax=ax,
        )

        ax.set_title(title or f"Mapa Espacial: {column}", fontsize=14)
        if legend_label:
            ax.annotate(
                legend_label, xy=(1.05, 1.05), xycoords="axes fraction", fontsize=10
            )

        ax.set_axis_off()
        plt.tight_layout()
        plt.show()

    def plot_scatter(
        self,
        x: str,
        y: str,
        size: Optional[str] = None,
        hue: Optional[str] = None,
        title: Optional[str] = None,
    ):
        fig, ax = plt.subplots(figsize=(10, 6))

        sizes = None
        if size:
            sizes = (self.gdf[size] / self.gdf[size].max()) * 500

        sns.scatterplot(
            data=self.gdf,
            x=x,
            y=y,
            size=sizes if size else None,
            hue=hue,
            alpha=0.6,
            sizes=(20, 500) if size else None,
            palette="Set2" if hue else None,
            ax=ax,
        )

        ax.set_title(title or f"Relação: {x} vs {y}", fontsize=14)

        if hue or size:
            plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")

        plt.tight_layout()
        plt.show()

    def plot_ranking_bars(
        self,
        cat_col: str,
        num_col: str,
        top_n: int = 15,
        agg_func: str = "sum",
        title: Optional[str] = None,
    ):
        ranked = (
            self.gdf.groupby(cat_col)[num_col]
            .agg(agg_func)
            .sort_values(ascending=False)
            .head(top_n)
            .reset_index()
        )

        plt.figure(figsize=(12, 8))
        sns.barplot(data=ranked, y=cat_col, x=num_col, palette="Blues_r")

        plt.title(
            title or f"Top {top_n} {cat_col} por {num_col} ({agg_func})", fontsize=14
        )
        plt.xlabel(num_col)
        plt.ylabel(cat_col)
        plt.tight_layout()
        plt.show()

    def plot_variability(
        self,
        cat_col: str,
        num_col: str,
        kind: Literal["box", "violin"] = "box",
        top_n_cats: Optional[int] = None,
        title: Optional[str] = None,
    ):
        data = self.gdf.copy()
        if top_n_cats:
            top_cats = data[cat_col].value_counts().nlargest(top_n_cats).index
            data = data[data[cat_col].isin(top_cats)]

        plt.figure(figsize=(12, 8))

        if kind == "box":
            sns.boxplot(data=data, y=cat_col, x=num_col, palette="pastel")
        else:
            sns.violinplot(
                data=data, y=cat_col, x=num_col, palette="pastel", inner="quartile"
            )

        plt.title(title or f"Variabilidade de {num_col} por {cat_col}", fontsize=14)
        plt.tight_layout()
        plt.show()

    def plot_distribution(
        self, column: str, hue: Optional[str] = None, title: Optional[str] = None
    ):
        plt.figure(figsize=(10, 6))

        sns.histplot(
            data=self.gdf,
            x=column,
            hue=hue,
            kde=True,
            element="step",
            palette="Set1" if hue else None,
            alpha=0.4,
        )

        plt.title(title or f"Distribuição de Frequência: {column}", fontsize=14)
        plt.tight_layout()
        plt.show()
