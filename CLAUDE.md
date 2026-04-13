# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Open Data Chunker is an ETL pipeline for processing large-scale XML datasets from the Italian RNA (Registro Nazionale degli Aiuti) Open Data. It transforms XML into Hive-partitioned Parquet (`public/parquet/{table}/ANNO=YYYY/`) and provides query, export, and AI classification capabilities.

## Common Commands

```bash
# Run tests
pytest tests/

# Run a single test file
pytest tests/test_parser.py

# Run via Docker (primary way)
docker compose run --rm etl pytest tests/

# Parse XML files
docker compose run --rm etl python -m src.cli parse --input data/ --workers 8

# Start the web UI (Go, port 3000)
docker compose up web

# Start inference service (requires GPU)
docker compose up inference-service

# Build all Docker services
docker compose build
```

The CLI entry point is `python -m src.cli` (Click-based). All commands: `parse`, `query`, `export`, `export-aggregated`, `classify`, `classify-cached`, `build-cache`, `clean-text`, `count-descriptions`.

## Architecture

The system has three services defined in `docker-compose.yml`:

- **etl** (Python 3.12) — XML parsing, Parquet writing, querying, exporting, classification. Entry point: `src/cli.py`.
- **web** (Go 1.24) — Browser-based query editor and export manager. Uses DuckDB in-process via `go-duckdb`. Single file: `web/main.go` serving `web/static/`.
- **inference-service** — Git submodule (`inference-service/`, repo: Pack-a-Punch). PyTorch-based text classifier for AI/Non-AI classification. Exposes `POST /classify` on port 8080.

### Data Flow

1. **Parse**: `src/parser.py` reads XML via `lxml.etree.iterparse()` with `CleanFileInputStream` (sanitizes invalid XML chars). Writes to three Parquet tables using PyArrow schemas from `src/models.py`.
2. **Query/Export**: `src/exporter.py` uses DuckDB for SQL queries and Polars for CSV/TXT export. `export_aggregated_dataset` joins all three tables year-by-year.
3. **Classification pipeline** (recommended order: `clean-text` → `build-cache` → `classify-cached`):
   - `src/classification_cache.py` extracts unique descriptions via DuckDB, classifies each once through the inference service, saves a Parquet cache, then joins the cache per-year with multithreading.
   - `src/classifier.py` is the older per-row classifier (slower, kept for direct use).

### Data Model (three normalized tables)

- **aiuti** — Main grants/subsidies records. Key fields: `CAR`, `COR` (composite key), `DESCRIZIONE_PROGETTO`, `ANNO` (partition column).
- **componenti** — Aid components linked to aiuti via `CAR_AIUTO`+`COR_AIUTO`.
- **strumenti** — Financial instruments linked to componenti via `ID_COMPONENTE_AIUTO`. Contains `IMPORTO_NOMINALE` and `ELEMENTO_DI_AIUTO` (monetary values).

Schemas are defined as PyArrow schemas in `src/models.py`. All tables are partitioned by `ANNO` (year extracted from `DATA_CONCESSIONE`).

### Key Patterns

- Parser uses `ProcessPoolExecutor` for parallel XML file processing; classification cache uses `ThreadPoolExecutor` for parallel year export.
- The parser batches records (`BATCH_SIZE = 10000`) before flushing to Parquet via `pyarrow.parquet.write_to_dataset`.
- `get_aggregated_year_lazyframe()` in `src/exporter.py` is the shared join logic used by both export-aggregated and classification commands.
- Tests use `unittest.mock.patch` to swap `DATA_DIR` paths and mock the inference service HTTP calls.

## Volumes and Paths

- `data/` — Input XML files (gitignored, mounted as Docker volume)
- `public/parquet/` — Output Parquet datasets
- `public/cache/` — Classification cache (`classification_cache.parquet`)
- `public/classified/` — Classified CSV exports
- `public/exports/` — General CSV/TXT exports

## Workflow Orchestration

### 1. Plan Node Default
-   Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
-   If something goes sideways, STOP and re-plan immediately - don't keep pushing
-   Use plan mode for verification steps, not just building
-   Write detailed specs upfront to reduce ambiguity

### 2. Subagent Strategy
-   Use subagents liberally to keep main contect window clean
-   Offload research, exploration, and parallel analysis to subagents
-   For complex problens, throw more compute at it via subagents
-   One tack per subagent for focused execution

### 3. Self-Improvement Loop
-   After ANY correction from the user: update 'tasks/lessons.md' with the pattern
-   Write rules for yourself that prevent the same mistake
-   Ruthlessly iterate on these lessons until mistake rate drops
-   Review lessons at session start for relevant project

### 4. Verification Before Done
-   Never mark a task complete without proving it works
-   Diff behavior between main and your changes when relevant
-   Ask yourself: "Would a staff engineer approve this?"
-   Run tests, check logs, demonstrate correctness

### 5. Demand Elegance (Balanced)
-   For non-trivial changes: pause and ask "is there a more elegant way?"
-   If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
-   Skip this for simple, chvious fixes - don't over-engineer
-   Challenge your own work before presenting it

### 6. Autonomous Bug Fixing
-   When given a bug report: just fix it. Don't ask for hand-holding
-   Point at logs,errors, failing tests - then resolve them
-   Zero context switching required from the user
-   Go fix failing CI tests without being told how

## Task Management
1.    **PLan First**: Write plan to 'tasks/todo.md' with checkable items
2.    **Verify Plan**: Check in before starting implementation
3.    **Track Progress**: Mark items complete as you go
4.    **Explain Changes**: High-level summary at each step
5.    **Document Results**: Add review section to 'tasks/todo.md"
6.    **Capture Lessons**: Update 'tasks/lessons. md' after corrections

## Core Principles
-   **Simplicity First**: Make every change as simple as possible. Inpact minimal code.
-   **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
-   **Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.