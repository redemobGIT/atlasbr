from pathlib import Path
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd

from roda.processing.demographics import (
    filter_states,
    filter_years,
    select_columns,
    reshape_for_plot,
)


def render_plot(plot_df, hue_order, title, height, out_path):
    """Render a high-quality plot for A4 PDF output."""
    a4_width = 11.7  # inches (landscape A4)
    dpi = 600

    plt.figure(figsize=(a4_width, height), dpi=dpi, constrained_layout=True)

    sns.set_context("talk", font_scale=1.2)

    sns.lineplot(
        data=plot_df,
        x="ANO",
        y="População",
        hue="Grupo",
        hue_order=hue_order,
        palette="Dark2",
    )
    plt.ylim(bottom=0, top=20_000)
    plt.xlim(plot_df["ANO"].min(), plot_df["ANO"].max())
    # plt.title(title, fontsize=18)
    plt.xlabel("Ano", fontsize=16)
    plt.ylabel("População (milhares)", fontsize=16)

    plt.legend(
        title="Grupo",
        bbox_to_anchor=(1, 1),
        loc="upper left",
        fontsize=14,
        title_fontsize=16,
    )

    plt.grid(True)

    formatter = FuncFormatter(lambda x, _: f"{int(x):,}")
    plt.gca().yaxis.set_major_formatter(formatter)

    # Adjust tick width and length to make ticks less bold/prominent
    plt.tick_params(axis="both", which="major", width=1, length=5)

    # Set tick label size using tick_params
    plt.tick_params(axis="both", which="major", labelsize=14)

    if out_path is not None:
        plt.savefig(out_path, dpi=dpi, bbox_inches="tight")

    plt.show()


def plot_population_trend(
    df,
    sigla="BR",
    by_age=False,
    gender=False,
    start=None,
    end=None,
    height=5,
    out_path=None,
):
    df = filter_states(df, sigla)
    df = filter_years(df, start, end)

    columns = select_columns(df, by_age=by_age, gender=gender)
    if not columns:
        print("No matching columns found.")
        return

    plot_df = reshape_for_plot(df, columns)
    hue_order = sorted(plot_df["Grupo"].unique())

    title = "Projeção Populacional"
    if isinstance(sigla, list):
        title += f" ({', '.join(sigla)})"
    else:
        title += f" – {sigla}"

    disagg = ", ".join(
        filter(
            None, ["Por Faixa Etária" if by_age else "", "Por Gênero" if gender else ""]
        )
    )
    if disagg:
        title += f" ({disagg})"

    if out_path is not None:
        filepath = Path(out_path) / f"projecao_genero-{gender}_idade-{by_age}.png"
    render_plot(plot_df, hue_order, title, height, out_path=filepath)
