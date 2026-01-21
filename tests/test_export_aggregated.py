import pytest
import polars as pl
import os
from src.exporter import export_aggregated_dataset
from unittest.mock import patch, MagicMock

@pytest.fixture
def mock_parquet_data(tmp_path):
    data_dir = tmp_path / "data"
    (data_dir / "aiuti").mkdir(parents=True)
    (data_dir / "componenti").mkdir(parents=True)
    (data_dir / "strumenti").mkdir(parents=True)
    
    # Create dummy AIUTI
    df_aiuti = pl.DataFrame({
        "CAR": ["CAR1", "CAR2"],
        "TITOLO_MISURA": ["Misura 1", "Misura 2"],
        "DES_TIPO_MISURA": ["Tipo 1", "Tipo 2"],
        "TITOLO_PROGETTO": ["Prog 1", "Prog 2"],
        "DESCRIZIONE_PROGETTO": ["Desc 1", "Desc 2"],
        "DATA_CONCESSIONE": ["2022-01-01", "2022-01-02"],
        "CUP": ["CUP1", "CUP2"],
        "DENOMINAZIONE_BENEFICIARIO": ["Ben 1", "Ben 2"],
        "CODICE_FISCALE_BENEFICIARIO": ["CF1", "CF2"],
        "DES_TIPO_BENEFICIARIO": ["Tipo Ben 1", "Tipo Ben 2"],
        "REGIONE_BENEFICIARIO": ["Reg 1", "Reg 2"],
        "COR": ["COR1", "COR2"],
        "ANNO": [2022, 2022],
        "COD_OBIETTIVO": ["OBJ1", "OBJ2"],
        "DES_OBIETTIVO": ["DesObj1", "DesObj2"],
        "FILE_SOURCE": ["file1.xml", "file2.xml"]
    })
    df_aiuti.write_parquet(data_dir / "aiuti/part.parquet")
    
    # Create dummy COMPONENTI
    df_comp = pl.DataFrame({
        "ID_COMPONENTE_AIUTO": ["COMP1", "COMP2", "COMP3"],
        "CAR_AIUTO": ["CAR1", "CAR1", "CAR2"],
        "COR_AIUTO": ["COR1", "COR1", "COR2"],
        "SETTORE_ATTIVITA": ["A.1", "B.2", "C.3"]
    })
    df_comp.write_parquet(data_dir / "componenti/part.parquet")
    
    # Create dummy STRUMENTI
    df_strum = pl.DataFrame({
        "ID_COMPONENTE_AIUTO": ["COMP1", "COMP1", "COMP2", "COMP3"],
        "COD_STRUMENTO": ["STR1", "STR2", "STR1", "STR3"],
        "IMPORTO_NOMINALE": [100.0, 200.0, 50.0, 300.0],
        "ELEMENTO_DI_AIUTO": [10.0, 20.0, 5.0, 30.0]
    })
    df_strum.write_parquet(data_dir / "strumenti/part.parquet")
    
    return data_dir

def test_export_aggregated(mock_parquet_data, tmp_path):
    output_file = tmp_path / "aggregated.csv"
    
    # Patch DATA_DIR to point to our mock data
    with patch("src.exporter.DATA_DIR", mock_parquet_data):
        export_aggregated_dataset(str(output_file))
    
    assert output_file.exists()
    
    # Verify content
    df_res = pl.read_csv(output_file)
    
    # Check schema
    expected_cols = [
        "CAR", "IMPORTO_NOMINALE_TOTALE", "ELEMENTO_DI_AIUTO_TOTALE", 
        "NUM_COMPONENTI", "NUM_STRUMENTI", "SETTORI_ATTIVITA", "COD_STRUMENTI"
    ]
    for col in expected_cols:
        assert col in df_res.columns
    
    # Verify aggregation for CAR1
    # CAR1 -> COMP1, COMP2
    # COMP1 -> STR1 (100+10), STR2 (200+20)
    # COMP2 -> STR1 (50+5)
    # Totals: Importo: 350, Aiuto: 35, NumComp: 2, NumStrum: 3
    # Settori: A.1|B.2 (or B.2|A.1)
    
    row1 = df_res.filter(pl.col("CAR") == "CAR1")
    assert row1["IMPORTO_NOMINALE_TOTALE"][0] == 350.0
    assert row1["ELEMENTO_DI_AIUTO_TOTALE"][0] == 35.0
    assert row1["NUM_COMPONENTI"][0] == 2
    assert row1["NUM_STRUMENTI"][0] == 3
    
    settori = row1["SETTORI_ATTIVITA"][0].split("|")
    assert "A.1" in settori and "B.2" in settori

    # Verify aggregation for CAR2
    # CAR2 -> COMP3 -> STR3 (300+30)
    row2 = df_res.filter(pl.col("CAR") == "CAR2")
    assert row2["IMPORTO_NOMINALE_TOTALE"][0] == 300.0
    assert row2["ELEMENTO_DI_AIUTO_TOTALE"][0] == 30.0
    assert row2["NUM_COMPONENTI"][0] == 1
    assert row2["NUM_STRUMENTI"][0] == 1
