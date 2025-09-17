from __future__ import annotations
from pathlib import Path
import geopandas as gpd
import typer
from . import clean, io, spatial

app = typer.Typer(add_completion=False, no_args_is_help=True)

@app.command()
def importar(dataset: str, ano: int = typer.Option(...)) -> None:
    if dataset.lower() == "rais":
        df = io.load_rais(ano)
        df = clean.coerce_dtypes(clean.standardize_ids(df))
        out = Path(f"rais_{ano}.parquet")
        df.to_parquet(out)
        typer.echo(f"OK: {out}")
    else:
        raise typer.BadParameter(f"Dataset não suportado: {dataset}")

@app.command()
def mapear_hex(entrada: str, saida: str, res: int = 9, ea_epsg: int = 5880) -> None:
    gdf = gpd.read_file(entrada)
    hexes = spatial.make_h3_grid(gdf, res, ea_epsg=ea_epsg)
    if Path(saida).suffix.lower() in {".gpkg"}:
        hexes.to_file(saida, driver="GPKG")
    else:
        hexes.to_file(saida)
    typer.echo(f"OK: {saida}")

if __name__ == "__main__":
    app()
