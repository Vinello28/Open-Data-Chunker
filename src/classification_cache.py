"""
Modulo per il caching intelligente della classificazione semantica.

Idea: le stesse descrizioni compaiono migliaia di volte nel dataset.
Classifichiamo ogni descrizione unica una sola volta, salviamo il risultato
in un file Parquet di cache, e poi facciamo un semplice join per l'export.
"""

import polars as pl
import duckdb
from pathlib import Path
import logging
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from .exporter import get_aggregated_year_lazyframe

logger = logging.getLogger(__name__)

DATA_DIR = Path("public/parquet")

# Colonne del file cache (binaria)
CACHE_SCHEMA = {
    "DESCRIZIONE_PROGETTO": pl.Utf8,
    "CLASSIFICAZIONE": pl.Utf8,
    "CLASSIFICAZIONE_CONFIDENZA": pl.Float64,
}

# Colonne del file cache (multiclasse)
MULTICLASS_CACHE_SCHEMA = {
    "DESCRIZIONE_PROGETTO": pl.Utf8,
    "CLASSIFICAZIONE_MULTICLASS": pl.Utf8,
    "CLASSIFICAZIONE_MULTICLASS_CONFIDENZA": pl.Float64,
}


def _classify_batch(batch: list[str], inference_url: str, timeout: int = 120) -> list[dict]:
    """Classifica un singolo batch di testi tramite l'inference-service."""
    try:
        response = requests.post(
            inference_url,
            json={"texts": batch},
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json().get("predictions", [])
    except Exception as e:
        logger.error(f"Batch inference failed: {e}")
        # Placeholder di errore per mantenere l'allineamento
        return [{"label": "ERROR", "confidence": 0.0} for _ in batch]


def build_classification_cache(
    output_path: str,
    batch_size: int = 32,
    inference_url: str = "http://inference-service:8080/classify",
):
    """
    Costruisce la cache di classificazione:
    1. Estrae le descrizioni uniche (GROUP BY via DuckDB)
    2. Classifica ciascuna descrizione una sola volta (sequenziale, modello singolo)
    3. Salva il risultato come Parquet

    Args:
        output_path: Path del file Parquet di cache da creare.
        batch_size: Dimensione dei batch per l'inference.
        inference_url: URL dell'endpoint /classify dell'inference-service.
    """
    dataset_path = str(DATA_DIR / "aiuti") + "/**/*.parquet"

    # 1. Estrazione descrizioni uniche con DuckDB (come count_project_descriptions)
    logger.info("Extracting unique descriptions via DuckDB...")
    con = duckdb.connect()
    try:
        query = f"""
            SELECT DESCRIZIONE_PROGETTO, COUNT(*) as conteggio
            FROM read_parquet('{dataset_path}')
            WHERE DESCRIZIONE_PROGETTO IS NOT NULL AND DESCRIZIONE_PROGETTO != ''
            GROUP BY DESCRIZIONE_PROGETTO
            ORDER BY conteggio DESC
        """
        result_arrow = con.execute(query).arrow()
        
        # DuckDB .arrow() può restituire un RecordBatchReader; convertiamo a Table
        if hasattr(result_arrow, 'read_all'):
            result_arrow = result_arrow.read_all()
        
        # Gestione dataset vuoto
        if result_arrow.num_rows == 0:
            logger.warning("No descriptions found. Cache not created.")
            return
        
        df_unique = pl.from_arrow(result_arrow)
    finally:
        con.close()

    total_unique = len(df_unique)
    logger.info(f"Found {total_unique} unique descriptions to classify.")

    texts = df_unique["DESCRIZIONE_PROGETTO"].to_list()

    # 2. Classificazione sequenziale (modello singolo, no beneficio da parallelismo)
    logger.info(f"Classifying {total_unique} unique descriptions with batch_size={batch_size}...")

    predictions = []

    with tqdm(total=total_unique, desc="Classifying unique descriptions") as pbar:
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            batch_preds = _classify_batch(batch, inference_url)
            predictions.extend(batch_preds)
            pbar.update(len(batch))

    # 3. Costruzione DataFrame e salvataggio cache
    labels = [p["label"] for p in predictions]
    confidences = [p["confidence"] for p in predictions]

    df_cache = pl.DataFrame({
        "DESCRIZIONE_PROGETTO": texts,
        "CLASSIFICAZIONE": labels,
        "CLASSIFICAZIONE_CONFIDENZA": confidences,
    })

    # Crea directory parent se necessaria
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    df_cache.write_parquet(output_path)
    logger.info(f"Classification cache saved to {output_path} ({total_unique} entries).")


def load_classification_cache(cache_path: str) -> pl.LazyFrame:
    """
    Carica la cache di classificazione come LazyFrame per join efficienti.

    Args:
        cache_path: Path al file Parquet di cache.

    Returns:
        LazyFrame con colonne DESCRIZIONE_PROGETTO, CLASSIFICAZIONE, CLASSIFICAZIONE_CONFIDENZA.

    Raises:
        FileNotFoundError: Se il file cache non esiste.
    """
    path = Path(cache_path)
    if not path.exists():
        raise FileNotFoundError(f"Classification cache not found: {cache_path}")

    return pl.scan_parquet(cache_path)


def _process_year(year_val: int, year_path: Path, lf_cache: pl.LazyFrame, output_dir: Path):
    """
    Processa un singolo anno: join con la cache + export CSV.
    Funzione worker estraibile per il multithreading.
    """
    try:
        lf_aiuti = get_aggregated_year_lazyframe(year_val)
        if lf_aiuti is None:
             logger.warning(f"No aggregated data for year {year_val}.")
             return year_val, False

        # Left join con la cache sulla DESCRIZIONE_PROGETTO
        lf_classified = lf_aiuti.join(
            lf_cache,
            on="DESCRIZIONE_PROGETTO",
            how="left",
        )

        # Per le descrizioni NULL o non presenti in cache, mettiamo un default
        lf_classified = lf_classified.with_columns([
            pl.col("CLASSIFICAZIONE").fill_null("UNKNOWN"),
            pl.col("CLASSIFICAZIONE_CONFIDENZA").fill_null(0.0),
        ])

        # Export CSV
        out_file = output_dir / f"classified_aiuti_{year_val}.csv"
        lf_classified.collect(engine="streaming").write_csv(out_file, quote_style="necessary")
        logger.info(f"Exported {out_file}")
        return year_val, True

    except Exception as e:
        logger.error(f"Failed processing year {year_val}: {e}")
        return year_val, False


def classify_with_cache(
    output_path: str,
    cache_path: str,
    year: int = None,
    max_workers: int = 4,
):
    """
    Classifica i progetti usando la cache pre-costruita.
    Per ogni anno, effettua un left join con la cache su DESCRIZIONE_PROGETTO
    e esporta il CSV risultante. Gli anni vengono processati in parallelo
    con multithreading per massimizzare la velocità di I/O.

    Args:
        output_path: Directory di output per i CSV classificati.
        cache_path: Path al file Parquet di cache.
        year: Anno specifico da processare (opzionale, tutti se None).
        max_workers: Numero di thread per il join+export parallelo per anno.
    """
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    aiuti_dir = DATA_DIR / "aiuti"
    if not aiuti_dir.exists():
        logger.error(f"Data directory {aiuti_dir} not found.")
        return

    # Caricamento cache
    logger.info(f"Loading classification cache from {cache_path}...")
    lf_cache = load_classification_cache(cache_path)

    # Discovery anni disponibili
    years_paths = list(aiuti_dir.glob("ANNO=*"))
    years_to_process = []

    for p in years_paths:
        try:
            y = int(p.name.split("=")[1])
            if year is None or y == year:
                years_to_process.append((y, p))
        except ValueError:
            continue

    years_to_process.sort(key=lambda x: x[0])

    if not years_to_process:
        logger.warning("No data found for year(s) specified.")
        return

    logger.info(f"Found {len(years_to_process)} years to process: {[y for y, _ in years_to_process]}")
    logger.info(f"Using {max_workers} threads for parallel join+export.")

    # Multithreading: ogni anno viene processato (join + export) in un thread separato
    with tqdm(total=len(years_to_process), desc="Exporting classified years") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_process_year, yv, yp, lf_cache, output_dir): yv
                for yv, yp in years_to_process
            }

            for future in as_completed(futures):
                year_val, success = future.result()
                status = "✓" if success else "✗"
                pbar.set_postfix_str(f"Year {year_val} {status}")
                pbar.update(1)

    logger.info("Cached classification export completed.")


def build_multiclass_cache(
    output_path: str,
    binary_cache_path: str,
    batch_size: int = 32,
    inference_url: str = "http://inference-service:8080/classify",
):
    """
    Costruisce la cache di classificazione multiclasse partendo dalla cache binaria.
    Prende solo le descrizioni classificate come "AI" nella cache binaria,
    le riclassifica con il modello multiclasse e salva il risultato come Parquet.

    Args:
        output_path: Path del file Parquet di cache multiclasse da creare.
        binary_cache_path: Path al file Parquet della cache binaria.
        batch_size: Dimensione dei batch per l'inference.
        inference_url: URL dell'endpoint /classify dell'inference-service multiclasse.
    """
    logger.info(f"Loading binary cache from {binary_cache_path}...")
    lf_binary = load_classification_cache(binary_cache_path)

    # Filtra solo le descrizioni classificate come AI
    df_ai = lf_binary.filter(pl.col("CLASSIFICAZIONE") == "AI").collect()

    total_ai = len(df_ai)
    if total_ai == 0:
        logger.warning("No AI-classified descriptions found in binary cache. Multiclass cache not created.")
        return

    logger.info(f"Found {total_ai} AI-classified descriptions to reclassify with multiclass model.")

    texts = df_ai["DESCRIZIONE_PROGETTO"].to_list()

    # Classificazione sequenziale in batch
    logger.info(f"Classifying {total_ai} descriptions with batch_size={batch_size}...")

    predictions = []

    with tqdm(total=total_ai, desc="Classifying (multiclass)") as pbar:
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            batch_preds = _classify_batch(batch, inference_url)
            predictions.extend(batch_preds)
            pbar.update(len(batch))

    labels = [p["label"] for p in predictions]
    confidences = [p["confidence"] for p in predictions]

    df_cache = pl.DataFrame({
        "DESCRIZIONE_PROGETTO": texts,
        "CLASSIFICAZIONE_MULTICLASS": labels,
        "CLASSIFICAZIONE_MULTICLASS_CONFIDENZA": confidences,
    })

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    df_cache.write_parquet(output_path)
    logger.info(f"Multiclass classification cache saved to {output_path} ({total_ai} entries).")


def _process_year_multiclass(
    year_val: int,
    year_path: Path,
    lf_binary_cache: pl.LazyFrame,
    lf_multiclass_cache: pl.LazyFrame,
    output_dir: Path,
):
    """
    Processa un singolo anno: double join (cache binaria + multiclasse) + export CSV.
    """
    try:
        lf_aiuti = get_aggregated_year_lazyframe(year_val)
        if lf_aiuti is None:
            logger.warning(f"No aggregated data for year {year_val}.")
            return year_val, False

        # Left join con cache binaria
        lf_classified = lf_aiuti.join(
            lf_binary_cache,
            on="DESCRIZIONE_PROGETTO",
            how="left",
        )

        # Left join con cache multiclasse
        lf_classified = lf_classified.join(
            lf_multiclass_cache,
            on="DESCRIZIONE_PROGETTO",
            how="left",
        )

        # Fill null solo per le colonne binarie (le multiclasse restano NULL per i NON-AI)
        lf_classified = lf_classified.with_columns([
            pl.col("CLASSIFICAZIONE").fill_null("UNKNOWN"),
            pl.col("CLASSIFICAZIONE_CONFIDENZA").fill_null(0.0),
        ])

        out_file = output_dir / f"classified_multiclass_aiuti_{year_val}.csv"
        lf_classified.collect(engine="streaming").write_csv(out_file, quote_style="necessary")
        logger.info(f"Exported {out_file}")
        return year_val, True

    except Exception as e:
        logger.error(f"Failed processing year {year_val}: {e}")
        return year_val, False


def classify_with_multiclass_cache(
    output_path: str,
    binary_cache_path: str,
    multiclass_cache_path: str,
    year: int = None,
    max_workers: int = 4,
):
    """
    Classifica i progetti con etichette multiclasse e esporta CSV.
    Per ogni anno, effettua un double left join (cache binaria + multiclasse)
    su DESCRIZIONE_PROGETTO e esporta il CSV risultante.

    Args:
        output_path: Directory di output per i CSV classificati.
        binary_cache_path: Path al file Parquet della cache binaria.
        multiclass_cache_path: Path al file Parquet della cache multiclasse.
        year: Anno specifico da processare (opzionale, tutti se None).
        max_workers: Numero di thread per il join+export parallelo per anno.
    """
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    aiuti_dir = DATA_DIR / "aiuti"
    if not aiuti_dir.exists():
        logger.error(f"Data directory {aiuti_dir} not found.")
        return

    logger.info(f"Loading binary cache from {binary_cache_path}...")
    lf_binary_cache = load_classification_cache(binary_cache_path)

    logger.info(f"Loading multiclass cache from {multiclass_cache_path}...")
    lf_multiclass_cache = load_classification_cache(multiclass_cache_path)

    # Discovery anni disponibili
    years_paths = list(aiuti_dir.glob("ANNO=*"))
    years_to_process = []

    for p in years_paths:
        try:
            y = int(p.name.split("=")[1])
            if year is None or y == year:
                years_to_process.append((y, p))
        except ValueError:
            continue

    years_to_process.sort(key=lambda x: x[0])

    if not years_to_process:
        logger.warning("No data found for year(s) specified.")
        return

    logger.info(f"Found {len(years_to_process)} years to process: {[y for y, _ in years_to_process]}")
    logger.info(f"Using {max_workers} threads for parallel join+export.")

    with tqdm(total=len(years_to_process), desc="Exporting multiclass classified years") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _process_year_multiclass, yv, yp, lf_binary_cache, lf_multiclass_cache, output_dir
                ): yv
                for yv, yp in years_to_process
            }

            for future in as_completed(futures):
                year_val, success = future.result()
                status = "✓" if success else "✗"
                pbar.set_postfix_str(f"Year {year_val} {status}")
                pbar.update(1)

    logger.info("Multiclass classification export completed.")
