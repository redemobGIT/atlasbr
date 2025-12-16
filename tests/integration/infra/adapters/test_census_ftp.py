import io
import zipfile
import pytest
import pandas as pd
import requests
from atlasbr.core.catalog.census import CensusThemeSpec
from atlasbr.infra.adapters.census_ftp import fetch_census_ftp

# --- Fixtures (Fábrica de Dados Falsos) ---

@pytest.fixture
def mock_zip_bytes():
    """
    Cria um arquivo ZIP em memória contendo um CSV 'sujo' típico do IBGE.
    Simula:
    1. Separador de ponto e vírgula.
    2. Números com vírgula decimal ('123,45').
    3. Valores ausentes representados por 'X' ou '..'.
    4. IDs sem padding ('123' em vez de '000...123').
    """
    csv_content = """Cod_setor;V001;V002;Lixo
123;1000,50;X;DadoInutil
456;2000,00;..;DadoInutil
789;3000;10,5;DadoInutil"""
    
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        # IBGE costuma usar nomes aleatórios, o teste deve pegar qualquer .csv
        zf.writestr("Arquivo_IBGE_Qualquer.csv", csv_content)
    
    return zip_buffer.getvalue()

@pytest.fixture
def spec_mock():
    """Retorna um CensusThemeSpec configurado para o teste."""
    return CensusThemeSpec(
        theme="test_theme",
        year=2099,
        strategy="ftp_csv",
        url="http://fake-ibge.com/dados.zip",
        csv_sep=";",
        csv_decimal=",",
        csv_encoding="utf-8",
        column_map={
            "Cod_setor": "id_setor_censitario",
            "V001": "renda_media",
            "V002": "populacao"
        }
    )

# --- Testes Unitários ---

def test_fetch_census_ftp_success(mocker, mock_zip_bytes, spec_mock):
    """
    Cenário Feliz:
    - O download funciona (mockado).
    - O CSV é encontrado e lido.
    - As colunas são renomeadas.
    - IDs ganham padding (zfill).
    - Decimais (vírgula) viram float (ponto).
    - 'X' e '..' viram NaN.
    """
    # 1. Mock do requests.get para retornar nosso ZIP falso
    mock_response = mocker.Mock()
    mock_response.content = mock_zip_bytes
    mock_response.raise_for_status.return_value = None
    mocker.patch("requests.get", return_value=mock_response)

    # 2. Executa a função
    df = fetch_census_ftp(spec_mock)

    # 3. Asserções (Validação Profissional)
    
    # Valida estrutura do índice
    assert df.index.name == "id_setor_censitario"
    assert len(df) == 3
    
    # Valida Padding do ID (123 -> 000000000000123)
    assert df.index[0] == "000000000000123"
    
    # Valida Conversão Numérica (1000,50 -> 1000.5)
    assert df["renda_media"].dtype == "float64"
    assert df.loc["000000000000123", "renda_media"] == 1000.5
    
    # Valida Tratamento de Nulos ('X' -> NaN)
    assert pd.isna(df.loc["000000000000123", "populacao"])
    
    # Valida se colunas não mapeadas foram ignoradas (usecols)
    assert "Lixo" not in df.columns

def test_fetch_census_ftp_network_error(mocker, spec_mock):
    """
    Cenário de Erro: Falha na conexão ou Timeout.
    Deve lançar RuntimeError encapsulando o erro original.
    """
    mocker.patch("requests.get", side_effect=requests.RequestException("Timeout"))

    with pytest.raises(RuntimeError) as excinfo:
        fetch_census_ftp(spec_mock)
    
    assert "Failed to download Census data" in str(excinfo.value)

def test_fetch_census_ftp_no_csv_in_zip(mocker, spec_mock):
    """
    Cenário de Erro: O ZIP baixa com sucesso, mas não tem CSV dentro.
    """
    # Cria um ZIP vazio (apenas um arquivo .txt)
    empty_zip = io.BytesIO()
    with zipfile.ZipFile(empty_zip, "w") as zf:
        zf.writestr("leia_me.txt", "Texto inútil")
    
    mock_response = mocker.Mock()
    mock_response.content = empty_zip.getvalue()
    mocker.patch("requests.get", return_value=mock_response)

    with pytest.raises(FileNotFoundError, match="No CSV file found"):
        fetch_census_ftp(spec_mock)