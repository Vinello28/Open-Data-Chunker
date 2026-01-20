import os
from lxml import etree
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import List, Dict, Any, Tuple
import logging
import re
import io
from .models import SCHEMA_AIUTI, SCHEMA_COMPONENTI, SCHEMA_STRUMENTI

logger = logging.getLogger(__name__)

NS = "{http://www.rna.it/RNA_aiuto/schema}"

def clean_tag(tag: str) -> str:
    return tag.replace(NS, "")

def safe_text(element) -> str:
    if element is not None and element.text:
        return element.text.strip()
    return None

def safe_float(element) -> float:
    txt = safe_text(element)
    if txt:
        try:
            return float(txt)
        except ValueError:
            return 0.0
    return 0.0

class CleanFileInputStream:
    """
    Wrapper file-like che rimuove i caratteri XML non validi dallo stream.
    Rimuove i caratteri di controllo ASCII (0-31) eccetto \t (9), \n (10), \r (13).
    """
    def __init__(self, filename):
        self.f = open(filename, 'rb')
        # Regex per caratteri invalidi:
        # 1. Raw bytes: range 0x00-0x08, 0x0B-0x0C, 0x0E-0x1F
        # 2. Entità decimali: &#0; to &#31; (eccetto 9, 10, 13)
        # 3. Entità hex: &#x0; to &#x1F; (eccetto 9, A, D)
        # Nota: pattern bytes per performance
        self.invalid_xml_chars = re.compile(
            b'[\x00-\x08\x0b\x0c\x0e-\x1f]|'
            b'&#(?:[0-8]|1[1-2]|1[4-9]|2[0-9]|3[0-1]);|'
            b'&#x(?:[0-8]|[bB][cC]|[eE][fF]|1[0-9a-fA-F]);'
        )

    def read(self, size=-1):
        data = self.f.read(size)
        if not data:
            return data
        # Sostituisce i caratteri invalidi con spazio o nulla (qui usiamo nulla)
        return self.invalid_xml_chars.sub(b'', data)
    
    def close(self):
        self.f.close()
    
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def process_file(file_path: str, output_dir: str) -> Dict[str, int]:
    """
    Processa un singolo file XML e salva i risultati in Parquet partizionati per Anno.
    Restituisce statistiche sui record processati.
    """
    path = Path(file_path)
    filename = path.name
    
    stats = {"aiuti": 0, "componenti": 0, "strumenti": 0}
    
    try:
        # Usa il wrapper per pulire lo stream XML on-the-fly
        with CleanFileInputStream(str(path)) as clean_stream:
            # iterparse accetta un oggetto file-like
            # recover=True tenta di continuare anche se ci sono errori di parsing
            context = etree.iterparse(clean_stream, events=("end",), tag=f"{NS}AIUTO", recover=False)
            _process_xml_context(context, filename, output_dir, stats)
            
    except Exception as e:
        # Critical: convert exception to string to avoid pickling errors with lxml objects
        error_msg = str(e)
        logger.error(f"Critical error processing file {filename}: {error_msg}")
        return {"aiuti": 0, "componenti": 0, "strumenti": 0, "error": 1}

    return stats

def _process_xml_context(context, filename, output_dir, stats):
    """Logica estratta per processare il contesto XML"""
    batch_aiuti = []
    batch_componenti = []
    batch_strumenti = []
    
    BATCH_SIZE = 10000 
    
    for event, elem in context:
        try:
            # Estrazione dati AIUTO
            car = safe_text(elem.find(f"{NS}CAR"))
            cor = safe_text(elem.find(f"{NS}COR"))
            data_concessione = safe_text(elem.find(f"{NS}DATA_CONCESSIONE"))
            
            # Determinare l'anno per il partizionamento
            anno = 0
            if data_concessione and len(data_concessione) >= 4:
                try:
                    anno = int(data_concessione[:4])
                except:
                    anno = 0
            
            aiuto = {
                "CAR": car,
                "TITOLO_MISURA": safe_text(elem.find(f"{NS}TITOLO_MISURA")),
                "DES_TIPO_MISURA": safe_text(elem.find(f"{NS}DES_TIPO_MISURA")),
                "BASE_GIURIDICA_NAZIONALE": safe_text(elem.find(f"{NS}BASE_GIURIDICA_NAZIONALE")),
                "CODICE_FISCALE_BENEFICIARIO": safe_text(elem.find(f"{NS}CODICE_FISCALE_BENEFICIARIO")),
                "DENOMINAZIONE_BENEFICIARIO": safe_text(elem.find(f"{NS}DENOMINAZIONE_BENEFICIARIO")),
                "TITOLO_PROGETTO": safe_text(elem.find(f"{NS}TITOLO_PROGETTO")),
                "COR": cor,
                "DATA_CONCESSIONE": data_concessione,
                "ANNO": anno,
                "FILE_SOURCE": filename
            }
            batch_aiuti.append(aiuto)
            stats["aiuti"] += 1
            
            # Componenti
            componenti_node = elem.find(f"{NS}COMPONENTI_AIUTO")
            if componenti_node is not None:
                for comp_elem in componenti_node.findall(f"{NS}COMPONENTE_AIUTO"):
                    id_comp = safe_text(comp_elem.find(f"{NS}ID_COMPONENTE_AIUTO"))
                    
                    comp = {
                        "ID_COMPONENTE_AIUTO": id_comp,
                        "CAR_AIUTO": car,
                        "COR_AIUTO": cor,
                        "COD_PROCEDIMENTO": safe_text(comp_elem.find(f"{NS}COD_PROCEDIMENTO")),
                        "DES_PROCEDIMENTO": safe_text(comp_elem.find(f"{NS}DES_PROCEDIMENTO")),
                        "COD_REGOLAMENTO": safe_text(comp_elem.find(f"{NS}COD_REGOLAMENTO")),
                        "DES_REGOLAMENTO": safe_text(comp_elem.find(f"{NS}DES_REGOLAMENTO")),
                        "COD_OBIETTIVO": safe_text(comp_elem.find(f"{NS}COD_OBIETTIVO")),
                        "DES_OBIETTIVO": safe_text(comp_elem.find(f"{NS}DES_OBIETTIVO")),
                        "SETTORE_ATTIVITA": safe_text(comp_elem.find(f"{NS}SETTORE_ATTIVITA")),
                        "ANNO": anno
                    }
                    batch_componenti.append(comp)
                    stats["componenti"] += 1
                    
                    # Strumenti
                    strumenti_node = comp_elem.find(f"{NS}STRUMENTI_AIUTO")
                    if strumenti_node is not None:
                        for strum_elem in strumenti_node.findall(f"{NS}STRUMENTO_AIUTO"):
                            strum = {
                                "ID_COMPONENTE_AIUTO": id_comp,
                                "COD_STRUMENTO": safe_text(strum_elem.find(f"{NS}COD_STRUMENTO")),
                                "DES_STRUMENTO": safe_text(strum_elem.find(f"{NS}DES_STRUMENTO")),
                                "ELEMENTO_DI_AIUTO": safe_float(strum_elem.find(f"{NS}ELEMENTO_DI_AIUTO")),
                                "IMPORTO_NOMINALE": safe_float(strum_elem.find(f"{NS}IMPORTO_NOMINALE")),
                                "ANNO": anno
                            }
                            batch_strumenti.append(strum)
                            stats["strumenti"] += 1

            # Release memory for the processed element
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
                
            # Flush batches if size reached
            if len(batch_aiuti) >= BATCH_SIZE:
                flush_batches(batch_aiuti, batch_componenti, batch_strumenti, output_dir)
                batch_aiuti = []
                batch_componenti = []
                batch_strumenti = []

        except Exception as e:
            logger.error(f"Error processing element in {filename}: {e}")
            continue

    # Final flush
    if batch_aiuti:
        try:
            flush_batches(batch_aiuti, batch_componenti, batch_strumenti, output_dir)
        except Exception as e:
             logger.error(f"Error flushing final batch in {filename}: {str(e)}")
             
    # Non cancelliamo context qui perché è gestito dal chiamante, ma possiamo cancellare le ref
    del context

def flush_batches(aiuti: List[dict], componenti: List[dict], strumenti: List[dict], output_dir: str):
    """Scrive i batch su disco in formato Parquet partizionato dataset style"""
    base_path = Path(output_dir)
    
    if aiuti:
        df = pl.DataFrame(aiuti, schema=None, orient="row") 
        table = df.to_arrow()
        pq.write_to_dataset(
            table,
            root_path=str(base_path / "aiuti"),
            partition_cols=['ANNO'],
            existing_data_behavior='overwrite_or_ignore'
        )

    if componenti:
        df = pl.DataFrame(componenti, orient="row")
        table = df.to_arrow()
        pq.write_to_dataset(
            table,
            root_path=str(base_path / "componenti"),
            partition_cols=['ANNO'],
            existing_data_behavior='overwrite_or_ignore'
        )

    if strumenti:
        df = pl.DataFrame(strumenti, orient="row")
        table = df.to_arrow()
        pq.write_to_dataset(
            table,
            root_path=str(base_path / "strumenti"),
            partition_cols=['ANNO'],
            existing_data_behavior='overwrite_or_ignore'
        )
