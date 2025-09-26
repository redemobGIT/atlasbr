import json
from pathlib import Path
from typing import List, Tuple

def get_municipality_codes_from_names(
    cities: List[Tuple[str, str]], 
    json_path: Path = None
) -> List[int]:
    """
    Busca os códigos de municípios do IBGE a partir de uma lista de tuplas (UF, Nome da Cidade).

    Args:
        cities (List[Tuple[str, str]]): Uma lista de tuplas, onde cada tupla contém
            a sigla do estado e o nome da cidade. 
            Exemplo: [('RJ', 'Rio de Janeiro'), ('SP', 'São Paulo')].
        json_path (Path, optional): O caminho para o arquivo JSON de municípios. 
            Se não for fornecido, usará o caminho padrão.

    Returns:
        List[int]: Uma lista contendo os códigos IBGE dos municípios encontrados.
    """
    if json_path is None:
        # Constrói o caminho para o arquivo JSON a partir da localização deste script
        json_path = Path(__file__).parent.parent / "assets" / "municipalities.json"

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            municipalities_data = json.load(f)
    except FileNotFoundError:
        print(f"Erro: Arquivo JSON não encontrado em {json_path}")
        return []

    muni_codes = []
    for state, city_name in cities:
        try:
            code = municipalities_data[state.upper()][city_name]
            muni_codes.append(code)
        except KeyError:
            print(f"Aviso: O município '{city_name} - {state.upper()}' não foi encontrado no arquivo JSON e será ignorado.")
    
    return muni_codes


def generate_output_path(
    output_dir: Path,
    prefix: str,
    h3_resolution: int,
    muni_codes: List[int]
) -> Path:
    """
    Gera o caminho completo para o arquivo de saída Parquet.

    Args:
        output_dir (Path): O diretório onde o arquivo será salvo.
        prefix (str): O prefixo do nome do arquivo.
        h3_resolution (int): A resolução H3 utilizada.
        muni_codes (List[int]): A lista de códigos de municípios analisados.

    Returns:
        Path: O caminho completo para o arquivo de saída.
    """
    codes_str = "-".join(str(m) for m in sorted(muni_codes)) # Ordena os códigos para nomes consistentes
    filename = f"{prefix}_r{h3_resolution}_{codes_str}.parquet"
    return output_dir / filename