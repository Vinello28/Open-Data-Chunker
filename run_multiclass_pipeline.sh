#!/usr/bin/env bash
# run_multiclass_pipeline.sh
#
# Pipeline completa per classificazione multiclasse:
#   1. Training del modello multiclasse (inference-service/trainer)
#   2. Avvio del server di inferenza
#   3. (Opzionale) Build cache binaria + multiclasse + export CSV

set -euo pipefail

# ---------------------------------------------------------------------------
# Colori
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_ok()      { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_section() { echo -e "\n${BOLD}========== $* ==========${NC}"; }

# ---------------------------------------------------------------------------
# Directory radice del progetto (cartella del presente script)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFERENCE_COMPOSE="$SCRIPT_DIR/inference-service/docker/docker-compose.yml"
ROOT_COMPOSE="$SCRIPT_DIR/docker-compose.yml"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DATA_SOURCE="txt"
CSV_PATH=""
TEACHER_URL="http://localhost:1234/v1/chat/completions"
EXPORT_ONNX=false
GPU_ID="5"
SKIP_TRAINING=false
RUN_ETL=false
BINARY_CACHE_PATH="public/cache/classification_cache.parquet"
MULTICLASS_CACHE_PATH="public/cache/multiclass_cache.parquet"
ETL_OUTPUT_DIR="public/classified"
BATCH_SIZE=64
WORKERS=4
INFERENCE_URL_BINARY="http://inference-service:8080/classify"
INFERENCE_URL_MULTICLASS="http://inference-service:8080/classify"

# ---------------------------------------------------------------------------
usage() {
    cat <<EOF

${BOLD}Utilizzo:${NC} $0 [OPZIONI]

${BOLD}Opzioni training:${NC}
  --data-source txt|csv|distillation
                             Sorgente dati per il training (default: txt)
  --csv-path PATH            Path al CSV di training (necessario se --data-source csv)
  --teacher-url URL          URL del teacher LLM per distillation
                             (default: http://localhost:1234/v1/chat/completions)
  --export-onnx              Esporta il modello in ONNX dopo il training
  --gpu-id ID                GPU da usare nei container (default: 5)
  --skip-training            Salta il training, avvia solo l'inferenza

${BOLD}Opzioni ETL (opzionale):${NC}
  --run-etl                  Esegui anche build-cache + build-multiclass-cache + classify-multiclass
  --binary-cache PATH        Path cache binaria (default: public/cache/classification_cache.parquet)
  --multiclass-cache PATH    Path cache multiclasse (default: public/cache/multiclass_cache.parquet)
  --etl-output DIR           Directory output CSV classificati (default: public/classified)
  --batch-size N             Batch size per l'inferenza ETL (default: 64)
  --workers N                Thread per export parallelo (default: 4)
  --inference-url-binary URL URL endpoint classificazione binaria
  --inference-url-multi  URL URL endpoint classificazione multiclasse

  -h, --help                 Mostra questo messaggio

${BOLD}Esempi:${NC}
  # Training da file .txt → serve
  $0

  # Training da CSV + export ONNX → serve → build cache + export CSV
  $0 --data-source csv --csv-path inference-service/public/multiclass2_augmented.csv \\
     --export-onnx --run-etl

  # Solo serve (modello già addestrato)
  $0 --skip-training

  # Skip training + tutto ETL
  $0 --skip-training --run-etl

EOF
    exit 0
}

# ---------------------------------------------------------------------------
# Parsing argomenti
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-source)            DATA_SOURCE="$2"; shift 2 ;;
        --csv-path)               CSV_PATH="$2"; shift 2 ;;
        --teacher-url)            TEACHER_URL="$2"; shift 2 ;;
        --export-onnx)            EXPORT_ONNX=true; shift ;;
        --gpu-id)                 GPU_ID="$2"; shift 2 ;;
        --skip-training)          SKIP_TRAINING=true; shift ;;
        --run-etl)                RUN_ETL=true; shift ;;
        --binary-cache)           BINARY_CACHE_PATH="$2"; shift 2 ;;
        --multiclass-cache)       MULTICLASS_CACHE_PATH="$2"; shift 2 ;;
        --etl-output)             ETL_OUTPUT_DIR="$2"; shift 2 ;;
        --batch-size)             BATCH_SIZE="$2"; shift 2 ;;
        --workers)                WORKERS="$2"; shift 2 ;;
        --inference-url-binary)   INFERENCE_URL_BINARY="$2"; shift 2 ;;
        --inference-url-multi)    INFERENCE_URL_MULTICLASS="$2"; shift 2 ;;
        -h|--help)                usage ;;
        *) log_error "Opzione sconosciuta: $1"; usage ;;
    esac
done

# ---------------------------------------------------------------------------
# Validazione
# ---------------------------------------------------------------------------
if [[ "$DATA_SOURCE" == "csv" && -z "$CSV_PATH" ]]; then
    log_error "--data-source csv richiede --csv-path"
    exit 1
fi

if [[ ! -f "$INFERENCE_COMPOSE" ]]; then
    log_error "File docker-compose non trovato: $INFERENCE_COMPOSE"
    log_error "Assicurati di aver inizializzato il submodule: git submodule update --init"
    exit 1
fi

# ---------------------------------------------------------------------------
# STEP 1 — Training
# ---------------------------------------------------------------------------
if [[ "$SKIP_TRAINING" == false ]]; then
    log_section "STEP 1: Training modello multiclasse"

    # Costruzione comando train.py
    TRAIN_ARGS="--data-source $DATA_SOURCE"

    if [[ "$DATA_SOURCE" == "csv" ]]; then
        # Il container monta src/ e scripts/ ma non il root; il CSV deve essere
        # in un path accessibile dal container. Passiamo il path così com'è e
        # l'utente deve assicurarsi che sia montato.
        TRAIN_ARGS="$TRAIN_ARGS --csv-path $CSV_PATH"
    fi

    if [[ "$DATA_SOURCE" == "distillation" ]]; then
        TRAIN_ARGS="$TRAIN_ARGS --teacher-url $TEACHER_URL"
    fi

    if [[ "$EXPORT_ONNX" == true ]]; then
        TRAIN_ARGS="$TRAIN_ARGS --export-onnx"
    fi

    log_info "Avvio training con: python scripts/train.py $TRAIN_ARGS"
    log_info "GPU: $GPU_ID | docker-compose: $INFERENCE_COMPOSE"

    # Sovrascriviamo device_ids a runtime tramite env (non supportato direttamente
    # in Compose v2 senza modifica del file); usiamo PAP_TRAINING__ per config Pydantic.
    docker compose \
        -f "$INFERENCE_COMPOSE" \
        --profile training \
        run --rm \
        -e "PAP_TRAINING__BATCH_SIZE=8" \
        -e "PAP_TRAINING__FP16=true" \
        trainer \
        python scripts/train.py $TRAIN_ARGS

    log_ok "Training completato."
else
    log_warn "Training saltato (--skip-training)."
fi

# ---------------------------------------------------------------------------
# STEP 2 — Avvio inference-service
# ---------------------------------------------------------------------------
log_section "STEP 2: Avvio inference-service"

log_info "Ricostruzione immagine inference-service..."
docker compose -f "$ROOT_COMPOSE" build inference-service

log_info "Avvio inference-service in background..."
docker compose -f "$ROOT_COMPOSE" up -d inference-service

# Attesa health-check
log_info "Attesa che il server sia pronto (max 120s)..."
READY=false
for i in $(seq 1 24); do
    if docker compose -f "$ROOT_COMPOSE" exec -T inference-service \
        python -c "import httpx; httpx.get('http://localhost:8080/health').raise_for_status()" \
        2>/dev/null; then
        READY=true
        break
    fi
    log_info "  tentativo $i/24 — in attesa (5s)..."
    sleep 5
done

if [[ "$READY" == false ]]; then
    log_error "L'inference-service non è diventato healthy entro 120 secondi."
    log_error "Controlla i log: docker compose logs inference-service"
    exit 1
fi

log_ok "Inference-service pronto su http://localhost:8080"

# ---------------------------------------------------------------------------
# STEP 3 — ETL: build cache binaria + multiclasse + export CSV (opzionale)
# ---------------------------------------------------------------------------
if [[ "$RUN_ETL" == true ]]; then
    log_section "STEP 3: ETL — build cache e export multiclasse"

    # 3a. Build cache binaria (se non esiste già)
    if [[ -f "$SCRIPT_DIR/$BINARY_CACHE_PATH" ]]; then
        log_warn "Cache binaria già presente: $BINARY_CACHE_PATH — skip build-cache."
    else
        log_info "Build cache binaria..."
        docker compose -f "$ROOT_COMPOSE" run --rm etl \
            python -m src.cli build-cache \
            --output "$BINARY_CACHE_PATH" \
            --batch-size "$BATCH_SIZE" \
            --inference-url "$INFERENCE_URL_BINARY"
        log_ok "Cache binaria salvata: $BINARY_CACHE_PATH"
    fi

    # 3b. Build cache multiclasse
    log_info "Build cache multiclasse (solo record AI)..."
    docker compose -f "$ROOT_COMPOSE" run --rm etl \
        python -m src.cli build-multiclass-cache \
        --output "$MULTICLASS_CACHE_PATH" \
        --binary-cache "$BINARY_CACHE_PATH" \
        --batch-size "$BATCH_SIZE" \
        --inference-url "$INFERENCE_URL_MULTICLASS"
    log_ok "Cache multiclasse salvata: $MULTICLASS_CACHE_PATH"

    # 3c. Export CSV con classificazione binaria + multiclasse
    log_info "Export CSV classificati (binary + multiclass)..."
    docker compose -f "$ROOT_COMPOSE" run --rm etl \
        python -m src.cli classify-multiclass \
        --output "$ETL_OUTPUT_DIR" \
        --binary-cache "$BINARY_CACHE_PATH" \
        --multiclass-cache "$MULTICLASS_CACHE_PATH" \
        --workers "$WORKERS"
    log_ok "CSV esportati in: $ETL_OUTPUT_DIR"
fi

# ---------------------------------------------------------------------------
# Riepilogo
# ---------------------------------------------------------------------------
log_section "Pipeline completata"
echo ""
log_ok "inference-service attivo su http://localhost:8080"
if [[ "$RUN_ETL" == true ]]; then
    log_ok "Cache binaria:      $BINARY_CACHE_PATH"
    log_ok "Cache multiclasse:  $MULTICLASS_CACHE_PATH"
    log_ok "CSV classificati:   $ETL_OUTPUT_DIR"
fi
echo ""
log_info "Per fermare l'inference-service:"
echo "  docker compose stop inference-service"
echo ""
