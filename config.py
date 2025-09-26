import os
import json
from pathlib import Path
from dotenv import load_dotenv

# --- 0. Carregamento do Ambiente ---
# Carrega variáveis de um arquivo .env para o ambiente, se ele existir.
load_dotenv()


# --- 1. Definição dos Caminhos Principais ---
# Define os diretórios fundamentais do projeto de forma robusta e absoluta.
PROJECT_DIR = Path(__file__).resolve().parent
SRC_DIR = PROJECT_DIR / "src"
OUTPUTS_DIR = PROJECT_DIR / "outputs"
DATA_DIR = PROJECT_DIR / "data"
ASSETS_DIR = SRC_DIR / "roda" / "assets"


# --- 2. Parâmetros Globais e Segredos ---
# Constantes e segredos que são compartilhados por todo o projeto.
GCLOUD_PROJECT_ID = os.getenv("GCLOUD_PROJECT_ID")


# --- 3. Carregamento de Ativos Compartilhados (Shared Assets) ---
def _load_municipalities() -> dict:
    """Função interna para carregar o JSON de municípios de forma segura."""
    path = ASSETS_DIR / "municipalities.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ALERTA: Arquivo de municípios não encontrado em '{path}'.")
        return {}

MUNICIPALITIES = _load_municipalities()


# --- 4. Configurações Específicas por Análise --- #
# Cada dicionário contém caminhos mais específicos para organizar os resultados obtidos.

RAIS = {
    "output_dir": OUTPUTS_DIR / "rais",
    "data_dir": OUTPUTS_DIR / "rais" / "data",
    "figures_dir": OUTPUTS_DIR / "rais" / "figures",
}

SOCIODEMOGRAFIA = {
    "census_themes": ("basic", "income", "age", "race"),
    "h3_resolution": 9,
    "output_dir": OUTPUTS_DIR / "sociodemografia",
    "data_dir": OUTPUTS_DIR / "sociodemografia" / "data",
    "figures_dir": OUTPUTS_DIR / "sociodemografia" / "figures",
    "filename_prefix": "sociodemografia_hex",
}

ACESSIBILIDADE = {
    "output_dir": OUTPUTS_DIR / "acessibilidade",
    "data_dir": OUTPUTS_DIR / "acessibilidade" / "data",
    "figures_dir": OUTPUTS_DIR / "acessibilidade" / "figures",
    # Outros parâmetros...
}

GTFS = {
    "output_dir": OUTPUTS_DIR / "gtfs",
    "data_dir": OUTPUTS_DIR / "gtfs" / "data",
    "figures_dir": OUTPUTS_DIR / "gtfs" / "figures",
    # Outros parâmetros...
}


# --- 5. Ações de Configuração Inicial (Setup Actions) ---

_analysis_configs = [RAIS, SOCIODEMOGRAFIA, ACESSIBILIDADE, GTFS]
_dir_keys_to_create = ["output_dir", "data_dir", "figures_dir"]

for config in _analysis_configs:
    for key in _dir_keys_to_create:
        if key in config:
            config[key].mkdir(parents=True, exist_ok=True)