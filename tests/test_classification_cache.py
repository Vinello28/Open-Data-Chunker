"""
Test per il modulo classification_cache.

Verifica:
1. build_classification_cache: estrazione descrizioni uniche, chiamata inference, salvataggio Parquet
2. classify_with_cache: join cache con dati aiuti, export CSV corretto
"""
import pytest
import polars as pl
from pathlib import Path
from unittest.mock import patch, MagicMock
from src.classification_cache import (
    build_classification_cache,
    load_classification_cache,
    classify_with_cache,
)


@pytest.fixture
def mock_aiuti_data(tmp_path):
    """Crea dati aiuti fittizi con descrizioni duplicate in struttura partitioned."""
    aiuti_dir = tmp_path / "data" / "aiuti" / "ANNO=2023"
    aiuti_dir.mkdir(parents=True)

    # Descrizioni: "Progetto AI" appare 3 volte, "Progetto Generico" appare 2 volte
    df = pl.DataFrame({
        "CAR": ["C1", "C2", "C3", "C4", "C5"],
        "COR": ["R1", "R2", "R3", "R4", "R5"],
        "TITOLO_PROGETTO": ["T1", "T2", "T3", "T4", "T5"],
        "DESCRIZIONE_PROGETTO": [
            "Progetto AI",
            "Progetto Generico",
            "Progetto AI",
            "Progetto AI",
            "Progetto Generico",
        ],
        "DATA_CONCESSIONE": ["2023-01-01"] * 5,
        "FILE_SOURCE": ["test.xml"] * 5,
        "TITOLO_MISURA": ["M1"] * 5,
        "DES_TIPO_MISURA": ["TM1"] * 5,
        "CUP": ["CUP1"] * 5,
        "DENOMINAZIONE_BENEFICIARIO": ["Ben1"] * 5,
        "CODICE_FISCALE_BENEFICIARIO": ["CF1"] * 5,
        "DES_TIPO_BENEFICIARIO": ["TB1"] * 5,
        "REGIONE_BENEFICIARIO": ["Lazio"] * 5,
    })
    df.write_parquet(aiuti_dir / "part.parquet")

    return tmp_path / "data"


def _mock_post_classify(url, json, timeout=60):
    """Mock del POST all'inference-service: simula risposte di classificazione."""
    texts = json["texts"]
    predictions = []
    for text in texts:
        if "AI" in text.upper():
            predictions.append({"label": "AI", "confidence": 0.95})
        else:
            predictions.append({"label": "NON_AI", "confidence": 0.88})

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = lambda: None
    mock_resp.json.return_value = {"predictions": predictions}
    return mock_resp


class TestBuildClassificationCache:
    def test_creates_cache_with_unique_descriptions(self, mock_aiuti_data, tmp_path):
        cache_path = tmp_path / "cache" / "classification_cache.parquet"

        with patch("src.classification_cache.DATA_DIR", mock_aiuti_data):
            with patch("src.classification_cache.requests.post", side_effect=_mock_post_classify):
                build_classification_cache(
                    output_path=str(cache_path),
                    batch_size=10,
                    inference_url="http://fake:8080/classify",
                )

        assert cache_path.exists()

        df_cache = pl.read_parquet(cache_path)

        # Solo 2 descrizioni uniche su 5 righe originali
        assert len(df_cache) == 2

        # Colonne corrette
        assert "DESCRIZIONE_PROGETTO" in df_cache.columns
        assert "CLASSIFICAZIONE" in df_cache.columns
        assert "CLASSIFICAZIONE_CONFIDENZA" in df_cache.columns

        # Verifica classificazione
        ai_row = df_cache.filter(pl.col("DESCRIZIONE_PROGETTO") == "Progetto AI")
        assert ai_row["CLASSIFICAZIONE"][0] == "AI"

        non_ai_row = df_cache.filter(pl.col("DESCRIZIONE_PROGETTO") == "Progetto Generico")
        assert non_ai_row["CLASSIFICAZIONE"][0] == "NON_AI"

    def test_empty_dataset(self, tmp_path):
        """Verifica che non crashi su dataset vuoto."""
        data_dir = tmp_path / "empty_data" / "aiuti" / "ANNO=2023"
        data_dir.mkdir(parents=True)

        # DataFrame vuoto con schema corretto
        df = pl.DataFrame({
            "DESCRIZIONE_PROGETTO": pl.Series([], dtype=pl.Utf8),
        })
        df.write_parquet(data_dir / "part.parquet")

        cache_path = tmp_path / "cache" / "empty_cache.parquet"

        with patch("src.classification_cache.DATA_DIR", tmp_path / "empty_data"):
            build_classification_cache(
                output_path=str(cache_path),
                batch_size=10,
                inference_url="http://fake:8080/classify",
            )

        # Nessun file cache creato se vuoto
        assert not cache_path.exists()


class TestClassifyWithCache:
    def test_join_produces_correct_csv(self, mock_aiuti_data, tmp_path):
        # 1. Crea cache fittizia
        cache_path = tmp_path / "cache" / "classification_cache.parquet"
        cache_path.parent.mkdir(parents=True)

        df_cache = pl.DataFrame({
            "DESCRIZIONE_PROGETTO": ["Progetto AI", "Progetto Generico"],
            "CLASSIFICAZIONE": ["AI", "NON_AI"],
            "CLASSIFICAZIONE_CONFIDENZA": [0.95, 0.88],
        })
        df_cache.write_parquet(cache_path)

        # 2. Classifica con la cache
        output_dir = tmp_path / "classified"

        with patch("src.exporter.DATA_DIR", mock_aiuti_data), \
             patch("src.classification_cache.DATA_DIR", mock_aiuti_data):
            classify_with_cache(
                output_path=str(output_dir),
                cache_path=str(cache_path),
            )

        # 3. Verifica output
        out_file = output_dir / "classified_aiuti_2023.csv"
        assert out_file.exists()

        df_result = pl.read_csv(out_file, infer_schema_length=10000)

        # Tutte e 5 le righe originali
        assert len(df_result) == 5

        # Colonne di classificazione presenti
        assert "CLASSIFICAZIONE" in df_result.columns
        assert "CLASSIFICAZIONE_CONFIDENZA" in df_result.columns

        # Verifica che le label siano corrette per ogni riga
        ai_rows = df_result.filter(pl.col("DESCRIZIONE_PROGETTO") == "Progetto AI")
        assert len(ai_rows) == 3
        assert all(l == "AI" for l in ai_rows["CLASSIFICAZIONE"].to_list())

        non_ai_rows = df_result.filter(pl.col("DESCRIZIONE_PROGETTO") == "Progetto Generico")
        assert len(non_ai_rows) == 2
        assert all(l == "NON_AI" for l in non_ai_rows["CLASSIFICAZIONE"].to_list())

    def test_missing_descriptions_get_unknown(self, tmp_path):
        """Le descrizioni NULL/assenti dalla cache devono ottenere 'UNKNOWN'."""
        # Dati con una descrizione non in cache
        aiuti_dir = tmp_path / "data" / "aiuti" / "ANNO=2024"
        aiuti_dir.mkdir(parents=True)

        df = pl.DataFrame({
            "CAR": ["C1"],
            "COR": ["R1"],
            "TITOLO_PROGETTO": ["T1"],
            "DESCRIZIONE_PROGETTO": ["Descrizione Sconosciuta"],
            "DATA_CONCESSIONE": ["2024-01-01"],
            "FILE_SOURCE": ["test.xml"],
        })
        df.write_parquet(aiuti_dir / "part.parquet")

        # Cache senza questa descrizione
        cache_path = tmp_path / "cache" / "classification_cache.parquet"
        cache_path.parent.mkdir(parents=True)
        df_cache = pl.DataFrame({
            "DESCRIZIONE_PROGETTO": ["Altra descrizione"],
            "CLASSIFICAZIONE": ["AI"],
            "CLASSIFICAZIONE_CONFIDENZA": [0.99],
        })
        df_cache.write_parquet(cache_path)

        output_dir = tmp_path / "classified"

        with patch("src.exporter.DATA_DIR", tmp_path / "data"), \
             patch("src.classification_cache.DATA_DIR", tmp_path / "data"):
            classify_with_cache(
                output_path=str(output_dir),
                cache_path=str(cache_path),
            )

        df_result = pl.read_csv(output_dir / "classified_aiuti_2024.csv", infer_schema_length=10000)
        assert df_result["CLASSIFICAZIONE"][0] == "UNKNOWN"
        assert df_result["CLASSIFICAZIONE_CONFIDENZA"][0] == 0.0

    def test_cache_not_found_raises(self, tmp_path):
        """Verifica che un FileNotFoundError venga sollevato se la cache non esiste."""
        with pytest.raises(FileNotFoundError):
            load_classification_cache(str(tmp_path / "nonexistent.parquet"))
