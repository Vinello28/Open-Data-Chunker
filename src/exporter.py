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

def export_aggregated_dataset(output_path: str, delimiter: str = ","):
    """
    Esporta il dataset aggregato unendo AIUTI, COMPONENTI e STRUMENTI.
    Esegue join e aggregazioni per produrre una riga per ogni AIUTO con totali calcolati.
    """
    try:
        logger.info("Starting aggregated export...")
        
        # 1. Caricamento Lazy dei dataset
        # Usiamo scan_parquet per efficienza
        # Utilizziamo union_by_name per gestire potenziali differenze di schema tra file vecchi e nuovi
        
        # Helper per caricare e normalizzare
        def scan_and_normalize(path, required_cols=[]):
            try:
                lf = pl.scan_parquet(path)
                # Verifica colonne mancanti e aggiungile come null per evitare crash su dataset misti
                current_cols = lf.collect_schema().names()
                for col in required_cols:
                    if col not in current_cols:
                        logger.warning(f"Column {col} missing in {path}, filling with nulls.")
                        lf = lf.with_columns(pl.lit(None).alias(col))
                return lf
            except Exception as ex:
                logger.warning(f"Could not scan {path}: {ex}")
                # Ritorna un LF vuoto con lo schema atteso se fallisce tutto (es. cartella vuota)
                # Ma per semplicità lasciamo fallire se proprio non c'è nulla
                raise ex

        # Colonne target per AIUTI (solo quelle che stanno effettivamente nella tabella AIUTI + ANNO che gestiamo a mano)
        required_aiuti = [
            "CAR", "TITOLO_MISURA", "DES_TIPO_MISURA", "TITOLO_PROGETTO", 
            "DESCRIZIONE_PROGETTO", "DATA_CONCESSIONE", "CUP", 
            "DENOMINAZIONE_BENEFICIARIO", "CODICE_FISCALE_BENEFICIARIO", 
            "DES_TIPO_BENEFICIARIO", "REGIONE_BENEFICIARIO", 
            "FILE_SOURCE", "COR"
        ]
        
        # Colonne che ci aspettiamo di avere DOPO il join per l'aggregazione
        # (Utile per debugging, ma non per scan_and_normalize degli aiuti)
        
        # 1. Discovery degli anni disponibili
        # Cerchiamo le cartelle ANNO=YYYY in public/parquet/aiuti
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
        
        # output_path gestito come prefisso se non finisce con .csv, o manipolato
        out_path_obj = Path(output_path)
        base_stem = out_path_obj.stem
        base_dir = out_path_obj.parent
        base_ext = out_path_obj.suffix if out_path_obj.suffix else ".csv"
        
        pbar = tqdm(years, desc="Exporting years")
        
        for year in pbar:
            pbar.set_description(f"Exporting Year {year}")
            
            # Aiuti Year Path
            aiuti_year_path = DATA_DIR / "aiuti" / f"ANNO={year}"
            if not aiuti_year_path.exists():
                 continue

            try:
                lf_aiuti = scan_and_normalize(str(aiuti_year_path / "*.parquet"), required_aiuti)
                
                # Aggiungiamo la colonna ANNO manualmente perché leggendo la partizione foglia non c'è
                lf_aiuti = lf_aiuti.with_columns(pl.lit(year).alias("ANNO"))
                
                # Per componenti e strumenti
                comp_year_path = DATA_DIR / "componenti" / f"ANNO={year}"
                strum_year_path = DATA_DIR / "strumenti" / f"ANNO={year}"
                
                # Se mancano componenti per quell'anno, usiamo dummy vuoto per permettere il join
                if comp_year_path.exists():
                    lf_componenti = pl.scan_parquet(str(comp_year_path / "*.parquet"))
                else:
                    # Schema dummy vuoto
                    lf_componenti = pl.LazyFrame(schema=lf_aiuti.collect_schema()) # placeholder errato
                    # Meglio: scan vuoto o gestire join
                    lf_componenti = None

                if strum_year_path.exists():
                     lf_strumenti = pl.scan_parquet(str(strum_year_path / "*.parquet"))
                else:
                     lf_strumenti = None


                # JOIN LOGIC
                # Se non ci sono componenti, l'aggregazione di aiuti sarà semplice
                
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
                elif lf_strumenti is not None and lf_componenti is None:
                     # Caso strano: strumenti senza componenti? Impossibile da schema, ma gestiamo
                     pass

                # Aggregazione
                group_cols = [c for c in required_aiuti if c in lf_joined.collect_schema().names()] # Usa solo colonne disponibili
                
                # Definiamo le aggregazioni dinamicamente a seconda se abbiamo fatto il join o no
                # Ma per semplicità usiamo quelle standard, che su colonne mancanti (se null) daranno null
                
                aggs = [
                    pl.col("IMPORTO_NOMINALE").sum().fill_null(0).alias("IMPORTO_NOMINALE_TOTALE"),
                    pl.col("ELEMENTO_DI_AIUTO").sum().fill_null(0).alias("ELEMENTO_DI_AIUTO_TOTALE"),
                ]
                
                if "ID_COMPONENTE_AIUTO" in lf_joined.collect_schema().names():
                     aggs.append(pl.col("ID_COMPONENTE_AIUTO").n_unique().alias("NUM_COMPONENTI"))
                else:
                     aggs.append(pl.lit(0).alias("NUM_COMPONENTI"))
                     
                if "COD_STRUMENTO" in lf_joined.collect_schema().names():
                     aggs.append(pl.col("COD_STRUMENTO").count().alias("NUM_STRUMENTI"))
                     aggs.append(pl.col("COD_STRUMENTO").filter(pl.col("COD_STRUMENTO").is_not_null()).unique().str.concat("|").alias("COD_STRUMENTI"))
                else:
                     aggs.append(pl.lit(0).alias("NUM_STRUMENTI"))
                     aggs.append(pl.lit(None).alias("COD_STRUMENTI"))

                if "SETTORE_ATTIVITA" in lf_joined.collect_schema().names():
                    aggs.append(pl.col("SETTORE_ATTIVITA").filter(pl.col("SETTORE_ATTIVITA").is_not_null()).unique().str.concat("|").alias("SETTORI_ATTIVITA"))
                else:
                    aggs.append(pl.lit(None).alias("SETTORI_ATTIVITA"))


                aggregated = lf_joined.group_by(group_cols).agg(aggs)
                
                # Output filename
                year_out = base_dir / f"{base_stem}_{year}{base_ext}"
                
                # Collect & Write
                # Usiamo collect con streaming per risparmiare memoria
                aggregated.collect(streaming=True).write_csv(year_out, separator=delimiter)
                
                # Cleanup Polars cache if needed
                del aggregated
                del lf_joined
                
            except Exception as e:
                logger.error(f"Failed exporting year {year}: {e}")
                # Non blocchiamo tutto, continuiamo con prossimo anno?
                # Meglio loggare e continuare
                continue
                
        logger.info("Aggregated export completed.")
        
    except Exception as e:
        logger.error(f"Aggregated export fatal error: {e}")
        raise e
        
    except Exception as e:
        logger.error(f"Aggregated export error: {e}")
        # Rilancia l'eccezione per farla vedere alla CLI
        raise e
