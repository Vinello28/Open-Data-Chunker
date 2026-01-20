# Open Data Chunker - Manuale d'uso

## Setup

Il progetto utilizza Docker per garantire la riproducibilità.

### Requisiti
- Docker Engine & Docker Compose

### Build
Costruisci l'immagine Docker:
```bash
docker compose build
```

---

## Utilizzo

Tutti i comandi vengono eseguiti tramite `src.cli` all'interno del container.

### 1. Parsing dati XML

Per processare file XML e convertirli in Parquet:

```bash
# Processare un singolo file
docker compose run --rm etl python -m src.cli parse --input data/2022/OpenData_Aiuti_2022_08.xml

# Processare intera cartella data/
docker compose run --rm etl python -m src.cli parse --input data/ --workers 8
```

I file Parquet verranno salvati in `public/parquet/{table}/ANNO=YYYY/`.

### 2. Query Interattive

Esegui query SQL sui dati processati:

```bash
# Esempio: Primi 5 aiuti
docker compose run --rm etl python -m src.cli query --table aiuti --limit 5

# Esempio: Totale agevolato per anno (sql custom)
docker compose run --rm etl python -m src.cli query --table strumenti --query "SELECT ANNO, SUM(ELEMENTO_DI_AIUTO) as tot FROM read_parquet('public/parquet/strumenti/**/*.parquet') GROUP BY ANNO"
```

### 3. Esportazione CSV/TXT

Esporta i dataset processati:

```bash
# Esporta aiuti in CSV
docker compose run --rm etl python -m src.cli export --table aiuti --format csv --output public/exports/aiuti.csv

# Esporta strumenti in TXT custom
docker compose run --rm etl python -m src.cli export --table strumenti --format txt --delimiter "|" --output public/exports/strumenti.txt
```
