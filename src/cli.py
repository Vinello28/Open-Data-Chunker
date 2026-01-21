import click
import multiprocessing
from pathlib import Path
import time
from typing import List
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from .parser import process_file
from .exporter import export_dataset, run_query, export_aggregated_dataset

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@click.group()
def cli():
    """Open Data Chunker ETL CLI"""
    pass

@cli.command()
@click.option('--input', '-i', required=True, help='Input directory or file path')
@click.option('--output', '-o', default='public/parquet', help='Output directory for Parquet files')
@click.option('--workers', '-w', default=4, help='Number of worker processes')
def parse(input, output, workers):
    """Parse XML files and convert to Parquet"""
    input_path = Path(input)
    output_path = Path(output)
    
    # Pulizia preventiva della cartella di output per evitare conflitti di schema o dati misti
    if output_path.exists():
        logger.info(f"Cleaning output directory {output_path}...")
        import shutil
        shutil.rmtree(output_path)
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    files = []
    if input_path.is_file():
        files.append(str(input_path))
    else:
        # Recursive search for XML files
        files = [str(p) for p in input_path.rglob("*.xml")]
    
    logger.info(f"Found {len(files)} XML files to process")
    logger.info(f"Starting processing with {workers} workers...")
    
    start_time = time.time()
    
    total_stats = {"aiuti": 0, "componenti": 0, "strumenti": 0}
    failed_files = []
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Map future to filename for error tracking
        futures = {executor.submit(process_file, f, str(output_path)): f for f in files}
        
        with tqdm(total=len(files), desc="Processing files") as pbar:
            for future in as_completed(futures):
                filename = futures[future]
                try:
                    stats = future.result()
                    if stats.get("error", 0) > 0:
                         failed_files.append(filename)
                    else:
                        for k, v in stats.items():
                            if k in total_stats:
                                total_stats[k] += v
                except Exception as e:
                    logger.error(f"Worker failed for {filename}: {e}")
                    failed_files.append(filename)
                finally:
                    pbar.update(1)
    
    elapsed = time.time() - start_time
    logger.info(f"Processing completed in {elapsed:.2f} seconds")
    logger.info(f"Total processed records: {total_stats}")
    
    if failed_files:
        failure_path = output_path.parent / "failures.txt"
        with open(failure_path, "w") as f:
            for fail in failed_files:
                f.write(f"{fail}\n")
        logger.warning(f"WARNING: {len(failed_files)} files failed. List saved to {failure_path}")
    else:
        logger.info("All files processed successfully.")

@cli.command()
@click.option('--table', '-t', required=True, type=click.Choice(['aiuti', 'componenti', 'strumenti']), help='Table to query')
@click.option('--query', '-q', required=False, help='SQL query to filter data (DuckDB syntax)')
@click.option('--limit', '-l', default=10, help='Limit results')
def query(table, query, limit):
    """Run interactive queries on dataset"""
    run_query(table, query, limit)

@cli.command()
@click.option('--table', '-t', required=True, type=click.Choice(['aiuti', 'componenti', 'strumenti']), help='Table to export')
@click.option('--format', '-f', type=click.Choice(['csv', 'txt']), default='csv', help='Output format')
@click.option('--output', '-o', required=True, help='Output file path')
@click.option('--delimiter', '-d', default=',', help='Delimiter for TXT/CSV')
def export(table, format, output, delimiter):
    """Export dataset to CSV or TXT"""
    export_dataset(table, format, output, delimiter)

@cli.command()
@click.option('--output', '-o', required=True, help='Output CSV file path')
@click.option('--delimiter', '-d', default=',', help='Delimiter for CSV')
def export_aggregated(output, delimiter):
    """Export aggregated dataset (AIUTI + COMP + STRUM) to CSV"""
    export_aggregated_dataset(output, delimiter)

if __name__ == '__main__':
    cli()
