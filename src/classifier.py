import polars as pl
from pathlib import Path
import logging
import requests
from tqdm import tqdm
import time
from .exporter import get_aggregated_year_lazyframe

logger = logging.getLogger(__name__)

DATA_DIR = Path("public/parquet/aiuti")

def classify_and_export(output_path: str, batch_size: int = 32, inference_url: str = "http://inference-service:8080/classify", year: int = None):
    """
    Classifies projects as AI or NON-AI using the inference service and exports to CSV.
    
    Args:
        output_path: Path to the output directory.
        batch_size: Number of texts to send in a single batch.
        inference_url: URL of the inference service endpoint.
        year: Optional year to filter processing.
    """
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not DATA_DIR.exists():
        logger.error(f"Data directory {DATA_DIR} not found.")
        return

    # Find years to process
    years_paths = list(DATA_DIR.glob("ANNO=*"))
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
        logger.warning(f"No data found for year(s) specified.")
        return

    logger.info(f"Found {len(years_to_process)} years to process: {[y for y, _ in years_to_process]}")
    
    for year_val, year_path in years_to_process:
        logger.info(f"Processing year {year_val}...")
        
        try:
            # 1. Read Data
            # We need TITOLO_PROGETTO and DESCRIZIONE_PROGETTO for classification
            # And minimal ID columns (CAR, COR) to keep track, plus others we want in output
            # For simplicity, we read everything and append the classification
            
            lf = get_aggregated_year_lazyframe(year_val)
            if lf is None:
                logger.warning(f"No aggregated data for year {year_val}.")
                continue
            
            # Collect to memory for batch processing using requests (Polars HTTP isn't native/easy for this batching)
            # Warning: for HUGE datasets this might OOM. But assuming reasonable year chunks.
            # If OOM becomes an issue, we should stream batches.
            df = lf.collect() 
            
            if df.is_empty():
                logger.warning(f"No data for year {year_val}.")
                continue
                
            # Prepare inputs
            # Handle missing descriptions: fill with empty string or title
            df = df.with_columns([
                pl.col("TITOLO_PROGETTO").fill_null(""),
                pl.col("DESCRIZIONE_PROGETTO").fill_null("")
            ])
            
            # Combine Title + Description for better context
            texts = (df["TITOLO_PROGETTO"] + " . " + df["DESCRIZIONE_PROGETTO"]).to_list()
            
            predictions = []
            
            # Batch Inference
            with tqdm(total=len(texts), desc=f"Classifying {year_val}") as pbar:
                for i in range(0, len(texts), batch_size):
                    batch = texts[i : i + batch_size]
                    
                    try:
                        response = requests.post(
                            inference_url, 
                            json={"texts": batch}, 
                            timeout=60 # Extended timeout for long batches
                        )
                        response.raise_for_status()
                        result = response.json()
                        
                        batch_preds = result.get("predictions", [])
                        predictions.extend(batch_preds)
                        
                    except Exception as e:
                        logger.error(f"Batch inference failed: {e}")
                        # Add failure placeholders to keep alignment
                        for _ in batch:
                            predictions.append({"label": "ERROR", "confidence": 0.0})
                    
                    pbar.update(len(batch))
            
            # Add results to DataFrame
            labels = [p["label"] for p in predictions]
            confidences = [p["confidence"] for p in predictions]
            
            df_classified = df.with_columns([
                pl.Series("CLASSIFICAZIONE", labels),
                pl.Series("CLASSIFICAZIONE_CONFIDENZA", confidences)
            ])
            
            # Export
            out_file = output_dir / f"classified_aiuti_{year_val}.csv"
            df_classified.write_csv(out_file, quote_style="necessary")
            logger.info(f"Exported {out_file}")
            
        except Exception as e:
            logger.error(f"Failed processing year {year_val}: {e}")
            continue

    logger.info("Classification task completed.")
