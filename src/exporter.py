import polars as pl
import duckdb
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

DATA_DIR = Path("public/parquet")

def get_dataset_path(table: str) -> str:
    return str(DATA_DIR / table)

def run_query(table: str, sql_query: str = None, limit: int = 10):
    """Esegue una query SQL su DuckDB usando i file Parquet"""
    dataset_path = get_dataset_path(table) + "/**/*.parquet"
    
    con = duckdb.connect()
    
    try:
        # Registra la vista su glob pattern per leggere tutte le partizioni
        query_base = f"SELECT * FROM read_parquet('{dataset_path}')"
        
        final_query = sql_query if sql_query else f"{query_base} LIMIT {limit}"
        
        # Se l'utente ha scritto una query custom ma senza specificare la tabella sorgente
        # cerchiamo di iniettarla (approccio semplice per CLI)
        if "FROM" not in final_query.upper():
             final_query = f"SELECT * FROM read_parquet('{dataset_path}') WHERE {final_query}"
             
        print(f"Executing: {final_query}")
        result_arrow = con.execute(final_query).arrow()
        print(pl.from_arrow(result_arrow))
        
    except Exception as e:
        logger.error(f"Query error: {e}")
    finally:
        con.close()

def export_dataset(table: str, format: str, output_path: str, delimiter: str = ","):
    """Esporta il dataset in CSV/TXT usando Polars"""
    dataset_path = get_dataset_path(table)
    
    try:
        # Lazy load del dataset partizionato
        # scan_parquet supporta hive partitioning automaticamente
        lf = pl.scan_parquet(str(Path(dataset_path) / "**/*.parquet"))
        
        if table == 'aiuti':
            comp_path = get_dataset_path('componenti')
            if Path(comp_path).exists():
                lf_comp = pl.scan_parquet(str(Path(comp_path) / "**/*.parquet"))
                lf_comp_agg = lf_comp.group_by(["CAR_AIUTO", "COR_AIUTO"]).agg([
                    pl.col("DES_OBIETTIVO").filter(pl.col("DES_OBIETTIVO").is_not_null()).unique().str.join("|").alias("OBIETTIVO")
                ])
                lf = lf.join(
                    lf_comp_agg,
                    left_on=["CAR", "COR"],
                    right_on=["CAR_AIUTO", "COR_AIUTO"],
                    how="left"
                )

        # Qui potremmo aggiungere filtri o aggregazioni se passati via CLI
        
        # Collect (attenzione alla RAM per dataset enormi, meglio streaming)
        # Per ora usiamo sink_csv che è memory efficient
        logger.info(f"Exporting {table} to {output_path}...")
        
        if format == 'csv' or format == 'txt':
            lf.sink_csv(output_path, separator=delimiter)
        
        logger.info("Export completed.")
        
    except Exception as e:
        logger.error(f"Export error: {e}")

def export_aggregated_dataset(output_path: str, delimiter: str = ","):
    """
    Esporta il dataset aggregato unendo AIUTI, COMPONENTI e STRUMENTI.
    Esegue join e aggregazioni per produrre una riga per ogni AIUTO con totali calcolati.
    """
    try:
        logger.info("Starting aggregated export...")
        
        # Discovery degli anni disponibili
        aiuti_path = DATA_DIR / "aiuti"
        years = []
        if aiuti_path.exists():
            for p in aiuti_path.glob("ANNO=*"):
                try:
                     y = int(p.name.split("=")[1])
                     years.append(y)
                except:
                    pass
        years = sorted(years)
        
        if not years:
            logger.warning("No data found to export.")
            return

        logger.info(f"Found years to export: {years}")
        
        from tqdm import tqdm
        
        out_path_obj = Path(output_path)
        base_stem = out_path_obj.stem
        base_dir = out_path_obj.parent
        base_ext = out_path_obj.suffix if out_path_obj.suffix else ".csv"
        
        pbar = tqdm(years, desc="Exporting years")
        
        for year in pbar:
            pbar.set_description(f"Exporting Year {year}")
            
            try:
                aggregated = get_aggregated_year_lazyframe(year)
                if aggregated is None:
                    continue
                
                # Output filename
                year_out = base_dir / f"{base_stem}_{year}{base_ext}"
                
                # Collect & Write
                aggregated.collect(streaming=True).write_csv(year_out, separator=delimiter)
                
                # Cleanup Polars cache if needed
                del aggregated
                
            except Exception as e:
                logger.error(f"Failed exporting year {year}: {e}")
                continue
                
        logger.info("Aggregated export completed.")
        
    except Exception as e:
        logger.error(f"Aggregated export fatal error: {e}")
        raise e

def _scan_and_normalize(path: str, required_cols: list) -> pl.LazyFrame:
    """Helper interno per caricare dataset in polars aggiungendo le colonne mancanti come null."""
    try:
        lf = pl.scan_parquet(path)
        current_cols = lf.collect_schema().names()
        for col in required_cols:
            if col not in current_cols:
                logger.warning(f"Column {col} missing in {path}, filling with nulls.")
                lf = lf.with_columns(pl.lit(None).alias(col))
        return lf
    except Exception as ex:
        logger.warning(f"Could not scan {path}: {ex}")
        raise ex

def get_aggregated_year_lazyframe(year: int) -> pl.LazyFrame:
    """
    Ritorna un LazyFrame polars che contiene i dati AIUTI uniti a COMPONENTI e STRUMENTI,
    aggregati per l'anno specificato. Se non esistono aiuti per l'anno, ritorna None.
    """
    required_aiuti = [
        "CAR", "TITOLO_MISURA", "DES_TIPO_MISURA", "TITOLO_PROGETTO", 
        "DESCRIZIONE_PROGETTO", "DATA_CONCESSIONE", "CUP", 
        "DENOMINAZIONE_BENEFICIARIO", "CODICE_FISCALE_BENEFICIARIO", 
        "DES_TIPO_BENEFICIARIO", "REGIONE_BENEFICIARIO", 
        "FILE_SOURCE", "COR"
    ]
    
    aiuti_year_path = DATA_DIR / "aiuti" / f"ANNO={year}"
    if not aiuti_year_path.exists():
         return None

    try:
        lf_aiuti = _scan_and_normalize(str(aiuti_year_path / "*.parquet"), required_aiuti)
        lf_aiuti = lf_aiuti.with_columns(pl.lit(year).alias("ANNO"))
        
        comp_year_path = DATA_DIR / "componenti" / f"ANNO={year}"
        strum_year_path = DATA_DIR / "strumenti" / f"ANNO={year}"
        
        if comp_year_path.exists():
            lf_componenti = pl.scan_parquet(str(comp_year_path / "*.parquet"))
        else:
            lf_componenti = None

        if strum_year_path.exists():
             lf_strumenti = pl.scan_parquet(str(strum_year_path / "*.parquet"))
        else:
             lf_strumenti = None

        if lf_componenti is not None:
             lf_joined = lf_aiuti.join(
                lf_componenti, 
                left_on=["CAR", "COR"], 
                right_on=["CAR_AIUTO", "COR_AIUTO"], 
                how="left",
                suffix="_COMP"
            )
        else:
            lf_joined = lf_aiuti
            
        if lf_strumenti is not None and lf_componenti is not None:
            lf_joined = lf_joined.join(
                lf_strumenti,
                left_on="ID_COMPONENTE_AIUTO",
                right_on="ID_COMPONENTE_AIUTO",
                how="left",
                suffix="_STRUM"
            )

        group_cols = [c for c in required_aiuti if c in lf_joined.collect_schema().names()]
        
        aggs = []
        if "IMPORTO_NOMINALE" in lf_joined.collect_schema().names():
            aggs.append(pl.col("IMPORTO_NOMINALE").sum().fill_null(0).alias("IMPORTO_NOMINALE_TOTALE"))
        else:
            aggs.append(pl.lit(0.0).alias("IMPORTO_NOMINALE_TOTALE"))
            
        if "ELEMENTO_DI_AIUTO" in lf_joined.collect_schema().names():
            aggs.append(pl.col("ELEMENTO_DI_AIUTO").sum().fill_null(0).alias("ELEMENTO_DI_AIUTO_TOTALE"))
        else:
            aggs.append(pl.lit(0.0).alias("ELEMENTO_DI_AIUTO_TOTALE"))
        
        if "ID_COMPONENTE_AIUTO" in lf_joined.collect_schema().names():
             aggs.append(pl.col("ID_COMPONENTE_AIUTO").n_unique().alias("NUM_COMPONENTI"))
        else:
             aggs.append(pl.lit(0).alias("NUM_COMPONENTI"))
             
        if "COD_STRUMENTO" in lf_joined.collect_schema().names():
             aggs.append(pl.col("COD_STRUMENTO").count().alias("NUM_STRUMENTI"))
             aggs.append(pl.col("COD_STRUMENTO").filter(pl.col("COD_STRUMENTO").is_not_null()).unique().str.join("|").alias("COD_STRUMENTI"))
        else:
             aggs.append(pl.lit(0).alias("NUM_STRUMENTI"))
             aggs.append(pl.lit(None).alias("COD_STRUMENTI"))

        if "SETTORE_ATTIVITA" in lf_joined.collect_schema().names():
            aggs.append(pl.col("SETTORE_ATTIVITA").filter(pl.col("SETTORE_ATTIVITA").is_not_null()).unique().str.join("|").alias("SETTORI_ATTIVITA"))
        else:
            aggs.append(pl.lit(None).alias("SETTORI_ATTIVITA"))

        if "DES_OBIETTIVO" in lf_joined.collect_schema().names():
            aggs.append(pl.col("DES_OBIETTIVO").filter(pl.col("DES_OBIETTIVO").is_not_null()).unique().str.join("|").alias("OBIETTIVO"))
        else:
            aggs.append(pl.lit(None).alias("OBIETTIVO"))

        aggregated = lf_joined.group_by(group_cols).agg(aggs)
        return aggregated
        
    except Exception as e:
        logger.error(f"Error computing aggregated lazyframe for year {year}: {e}")
        raise e        


def clean_parquet_texts():
    """Rimuove le virgole, le doppie virgolette e le nuove linee dai campi testo di tutti i dataset esportati in parquet."""
    logger.info("Starting text cleaning in Parquet files...")
    try:
        from tqdm import tqdm
        parquet_files = list(Path(DATA_DIR).rglob("*.parquet"))
        if not parquet_files:
            logger.warning("No Parquet files found for cleaning.")
            return

        pbar = tqdm(parquet_files, desc="Cleaning Parquet files")
        for file_path in pbar:
            try:
                df = pl.read_parquet(file_path)
                
                string_cols = [col_name for col_name, dtype in zip(df.columns, df.dtypes) if dtype in (pl.Utf8, pl.String, pl.Categorical)]
                
                if string_cols:
                    modifications = {}
                    for col in string_cols:
                        modifications[col] = pl.col(col).cast(pl.Utf8).str.replace_all(",", " ").str.replace_all('"', " ").str.replace_all(r"[\n\r]+", " ")
                    
                    df = df.with_columns(**modifications)
                    df.write_parquet(file_path)
            except Exception as e:
                logger.error(f"Error cleaning file {file_path}: {e}")
        
        logger.info("Text cleaning completed successfully.")
    except Exception as e:
         logger.error(f"Critical error during file cleaning: {e}")
         raise e

def count_project_descriptions(output_path: str = None, limit: int = None):
    """
    Esegue una query SQL per raggruppare le descrizioni dei progetti
    e contarne le occorrenze.
    """
    aiuti_path = get_dataset_path('aiuti') + "/**/*.parquet"
    comp_path = get_dataset_path('componenti') + "/**/*.parquet"
    con = duckdb.connect()
    
    # We check if componenti exist to adjust query
    has_comp = Path(get_dataset_path('componenti')).exists()
    
    try:
        if has_comp:
            query = f"""
                SELECT 
                    a.DESCRIZIONE_PROGETTO, 
                    COUNT(DISTINCT a.CAR || coalesce(a.COR, '')) as conteggio,
                    string_agg(DISTINCT c.DES_OBIETTIVO, '|') as OBIETTIVO
                FROM read_parquet('{aiuti_path}') a
                LEFT JOIN read_parquet('{comp_path}') c ON a.CAR = c.CAR_AIUTO AND a.COR = c.COR_AIUTO
                WHERE a.DESCRIZIONE_PROGETTO IS NOT NULL AND a.DESCRIZIONE_PROGETTO != ''
                GROUP BY a.DESCRIZIONE_PROGETTO
                ORDER BY conteggio DESC
            """
        else:
            query = f"""
                SELECT DESCRIZIONE_PROGETTO, COUNT(*) as conteggio
                FROM read_parquet('{aiuti_path}')
                WHERE DESCRIZIONE_PROGETTO IS NOT NULL AND DESCRIZIONE_PROGETTO != ''
                GROUP BY DESCRIZIONE_PROGETTO
                ORDER BY conteggio DESC
            """
            
        if limit:
            query += f" LIMIT {limit}"
            
        logger.info(f"Executing count query...")
        result_arrow = con.execute(query).arrow()
        import polars as pl
        result_df = pl.from_arrow(result_arrow)
        
        if output_path:
            logger.info(f"Saving counts to {output_path}...")
            # Automatically create parent directories to avoid OS error 2
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            result_df.write_csv(output_path)
            logger.info("Save completed.")
        else:
            print(result_df)
            
    except Exception as e:
        logger.error(f"Query error: {e}")
    finally:
        con.close()
