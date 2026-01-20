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
        result = con.execute(final_query).df()
        print(result)
        
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
        
        # Qui potremmo aggiungere filtri o aggregazioni se passati via CLI
        
        # Collect (attenzione alla RAM per dataset enormi, meglio streaming)
        # Per ora usiamo sink_csv che è memory efficient
        logger.info(f"Exporting {table} to {output_path}...")
        
        if format == 'csv' or format == 'txt':
            lf.sink_csv(output_path, separator=delimiter)
            
        logger.info("Export completed.")
        
    except Exception as e:
        logger.error(f"Export error: {e}")
