package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

var (
	dataDir       = envOr("DATA_DIR", "/app/public/parquet")
	exportsDir    = envOr("EXPORTS_DIR", "/app/public/exports")
	classifiedDir = envOr("CLASSIFIED_DIR", "/app/public/classified")
	cacheDir      = envOr("CACHE_DIR", "/app/public/cache")
	listenPort    = envOr("PORT", "3000")
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ---------------------------------------------------------------------------
// Export Job Tracking
// ---------------------------------------------------------------------------

type ExportJob struct {
	ID        string  `json:"id"`
	Type      string  `json:"type"`
	Status    string  `json:"status"` // pending, running, done, error
	Progress  float64 `json:"progress"`
	Message   string  `json:"message"`
	Filename  string  `json:"filename,omitempty"`
	CreatedAt string  `json:"created_at"`
}

var (
	jobs   = map[string]*ExportJob{}
	jobsMu sync.RWMutex
)

func newJob(jobType string) *ExportJob {
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	j := &ExportJob{
		ID:        id,
		Type:      jobType,
		Status:    "pending",
		CreatedAt: time.Now().Format(time.RFC3339),
	}
	jobsMu.Lock()
	jobs[id] = j
	jobsMu.Unlock()
	return j
}

func getJob(id string) *ExportJob {
	jobsMu.RLock()
	defer jobsMu.RUnlock()
	return jobs[id]
}

// ---------------------------------------------------------------------------
// DuckDB — persistent connection pool
// ---------------------------------------------------------------------------

// Global DB pool, initialized once at startup in main().
// database/sql handles connection pooling internally.
var dbPool *sql.DB

func initDuckDB() {
	var err error
	dbPool, err = sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}

	// Configure DuckDB to use all available CPU cores
	numThreads := runtime.NumCPU()
	_, err = dbPool.Exec(fmt.Sprintf("SET threads = %d", numThreads))
	if err != nil {
		log.Printf("Warning: could not set threads: %v", err)
	}

	// Increase memory limit (default is 80%% of RAM which is fine)
	log.Printf("DuckDB initialized: %d threads", numThreads)
}

// discoverYears returns sorted list of years found in a Hive-partitioned dir.
func discoverYears(tableDir string) []int {
	entries, err := os.ReadDir(tableDir)
	if err != nil {
		return nil
	}
	var years []int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		parts := strings.SplitN(e.Name(), "=", 2)
		if len(parts) == 2 && parts[0] == "ANNO" {
			if y, err := strconv.Atoi(parts[1]); err == nil {
				years = append(years, y)
			}
		}
	}
	sort.Ints(years)
	return years
}

// ---------------------------------------------------------------------------
// API Handlers
// ---------------------------------------------------------------------------

// --- GET /api/tables -------------------------------------------------------

type TableInfo struct {
	Name  string `json:"name"`
	Years []int  `json:"years"`
	Count int64  `json:"count"`
}

func handleTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tables := []string{"aiuti", "componenti", "strumenti"}
	var infos []TableInfo

	for _, t := range tables {
		tDir := filepath.Join(dataDir, t)
		years := discoverYears(tDir)
		var count int64
		glob := filepath.Join(tDir, "**", "*.parquet")
		row := dbPool.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s', union_by_name=true)", glob))
		_ = row.Scan(&count) // ignore error if no files
		infos = append(infos, TableInfo{Name: t, Years: years, Count: count})
	}

	jsonOK(w, infos)
}

// --- GET /api/schema/:table ------------------------------------------------

type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func handleSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	table := strings.TrimPrefix(r.URL.Path, "/api/schema/")
	if table == "" {
		jsonError(w, "table name required", 400)
		return
	}

	glob := filepath.Join(dataDir, table, "**", "*.parquet")

	rows, err := dbPool.Query(fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s', union_by_name=true) LIMIT 0", glob))
	if err != nil {
		jsonError(w, "schema error: "+err.Error(), 500)
		return
	}
	defer rows.Close()

	var cols []ColumnInfo
	for rows.Next() {
		var name, dtype string
		var null_, key, def_, extra sql.NullString
		if err := rows.Scan(&name, &dtype, &null_, &key, &def_, &extra); err != nil {
			continue
		}
		cols = append(cols, ColumnInfo{Name: name, Type: dtype})
	}

	jsonOK(w, cols)
}

// --- GET /api/templates ----------------------------------------------------

type QueryTemplate struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

func handleTemplates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	aiutiGlob := filepath.Join(dataDir, "aiuti", "**", "*.parquet")
	compGlob := filepath.Join(dataDir, "componenti", "**", "*.parquet")
	strumGlob := filepath.Join(dataDir, "strumenti", "**", "*.parquet")

	templates := []QueryTemplate{
		{
			Name:        "Conteggio record per anno",
			Description: "Conta il numero di aiuti per ogni anno disponibile.",
			SQL: fmt.Sprintf(
				"SELECT ANNO, COUNT(*) AS conteggio\nFROM read_parquet('%s', union_by_name=true)\nGROUP BY ANNO\nORDER BY ANNO",
				aiutiGlob),
		},
		{
			Name:        "Top 20 beneficiari per importo",
			Description: "I 20 beneficiari che hanno ricevuto l'importo nominale totale più alto.",
			SQL: fmt.Sprintf(
				"SELECT\n  a.DENOMINAZIONE_BENEFICIARIO,\n  a.CODICE_FISCALE_BENEFICIARIO,\n  COUNT(*) AS num_aiuti,\n  SUM(s.IMPORTO_NOMINALE) AS totale_importo\nFROM read_parquet('%s', union_by_name=true) a\nLEFT JOIN read_parquet('%s', union_by_name=true) c\n  ON a.CAR = c.CAR_AIUTO AND a.COR = c.COR_AIUTO\nLEFT JOIN read_parquet('%s', union_by_name=true) s\n  ON c.ID_COMPONENTE_AIUTO = s.ID_COMPONENTE_AIUTO\nGROUP BY a.DENOMINAZIONE_BENEFICIARIO, a.CODICE_FISCALE_BENEFICIARIO\nORDER BY totale_importo DESC NULLS LAST\nLIMIT 20",
				aiutiGlob, compGlob, strumGlob),
		},
		{
			Name:        "Distribuzione per regione",
			Description: "Conteggio aiuti per regione del beneficiario.",
			SQL: fmt.Sprintf(
				"SELECT REGIONE_BENEFICIARIO, COUNT(*) AS conteggio\nFROM read_parquet('%s', union_by_name=true)\nWHERE REGIONE_BENEFICIARIO IS NOT NULL\nGROUP BY REGIONE_BENEFICIARIO\nORDER BY conteggio DESC",
				aiutiGlob),
		},
		{
			Name:        "Totali per anno (importo + elemento di aiuto)",
			Description: "Somma degli importi nominali e degli elementi di aiuto per anno.",
			SQL: fmt.Sprintf(
				"SELECT\n  a.ANNO,\n  SUM(s.IMPORTO_NOMINALE) AS totale_importo_nominale,\n  SUM(s.ELEMENTO_DI_AIUTO) AS totale_elemento_aiuto,\n  COUNT(DISTINCT a.CAR || COALESCE(a.COR,'')) AS num_aiuti\nFROM read_parquet('%s', union_by_name=true) a\nLEFT JOIN read_parquet('%s', union_by_name=true) c\n  ON a.CAR = c.CAR_AIUTO AND a.COR = c.COR_AIUTO\nLEFT JOIN read_parquet('%s', union_by_name=true) s\n  ON c.ID_COMPONENTE_AIUTO = s.ID_COMPONENTE_AIUTO\nGROUP BY a.ANNO\nORDER BY a.ANNO",
				aiutiGlob, compGlob, strumGlob),
		},
		{
			Name:        "🔍 Ricerca imprese per codice fiscale / P.IVA",
			Description: "Restituisce tutti gli aiuti ricevuti dalle imprese i cui codici fiscali compaiono nella lista fornita. Sostituisci i codici di esempio con quelli reali.",
			SQL: fmt.Sprintf(
				"SELECT *\nFROM read_parquet('%s', union_by_name=true)\nWHERE CODICE_FISCALE_BENEFICIARIO IN (\n  '00000000000',\n  '11111111111',\n  '22222222222'\n)\nORDER BY ANNO, DENOMINAZIONE_BENEFICIARIO",
				aiutiGlob),
		},
	}

	jsonOK(w, templates)
}

// --- POST /api/query -------------------------------------------------------

type QueryRequest struct {
	SQL   string `json:"sql"`
	Limit int    `json:"limit"`
}

type QueryResponse struct {
	Columns  []string        `json:"columns"`
	Rows     [][]interface{} `json:"rows"`
	RowCount int             `json:"row_count"`
	Truncated bool           `json:"truncated"`
	Duration string          `json:"duration"`
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid json: "+err.Error(), 400)
		return
	}

	if strings.TrimSpace(req.SQL) == "" {
		jsonError(w, "sql is required", 400)
		return
	}

	// Safety: enforce preview limit
	previewLimit := req.Limit
	if previewLimit <= 0 || previewLimit > 1000 {
		previewLimit = 1000
	}

	// Block dangerous statements
	upper := strings.ToUpper(strings.TrimSpace(req.SQL))
	for _, kw := range []string{"DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "ATTACH"} {
		if strings.HasPrefix(upper, kw) {
			jsonError(w, "write operations are not allowed", 403)
			return
		}
	}

	start := time.Now()

	// Wrap query with limit for preview
	wrappedSQL := fmt.Sprintf("SELECT * FROM (%s) sub LIMIT %d", req.SQL, previewLimit+1)

	rows, err := dbPool.Query(wrappedSQL)
	if err != nil {
		jsonError(w, "query error: "+err.Error(), 400)
		return
	}
	defer rows.Close()

	colTypes, _ := rows.ColumnTypes()
	colNames := make([]string, len(colTypes))
	for i, ct := range colTypes {
		colNames[i] = ct.Name()
	}

	var allRows [][]interface{}
	scanArgs := make([]interface{}, len(colNames))
	scanVals := make([]interface{}, len(colNames))
	for i := range scanVals {
		scanArgs[i] = &scanVals[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			continue
		}
		row := make([]interface{}, len(colNames))
		for i, v := range scanVals {
			row[i] = formatValue(v)
		}
		allRows = append(allRows, row)
	}

	truncated := len(allRows) > previewLimit
	if truncated {
		allRows = allRows[:previewLimit]
	}

	resp := QueryResponse{
		Columns:   colNames,
		Rows:      allRows,
		RowCount:  len(allRows),
		Truncated: truncated,
		Duration:  time.Since(start).Round(time.Millisecond).String(),
	}

	jsonOK(w, resp)
}

// --- POST /api/export/csv --------------------------------------------------

type ExportCSVRequest struct {
	SQL      string `json:"sql"`
	Filename string `json:"filename"`
}

func handleExportCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ExportCSVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid json: "+err.Error(), 400)
		return
	}

	if strings.TrimSpace(req.SQL) == "" {
		jsonError(w, "sql is required", 400)
		return
	}

	// Sanitize filename
	fname := req.Filename
	if fname == "" {
		fname = fmt.Sprintf("query_export_%d.csv", time.Now().Unix())
	}
	fname = filepath.Base(fname) // prevent traversal
	if !strings.HasSuffix(fname, ".csv") {
		fname += ".csv"
	}

	outPath := filepath.Join(exportsDir, fname)

	// Ensure exports dir exists
	os.MkdirAll(exportsDir, 0755)

	// Use COPY TO for efficient CSV export
	copySQL := fmt.Sprintf("COPY (%s) TO '%s' (HEADER, DELIMITER ',')", req.SQL, outPath)
	start := time.Now()
	_, err := dbPool.Exec(copySQL)
	if err != nil {
		jsonError(w, "export error: "+err.Error(), 400)
		return
	}

	// Get file size
	fi, _ := os.Stat(outPath)
	size := int64(0)
	if fi != nil {
		size = fi.Size()
	}

	jsonOK(w, map[string]interface{}{
		"filename": fname,
		"path":     "/api/exports/download?file=" + fname,
		"size":     size,
		"duration": time.Since(start).Round(time.Millisecond).String(),
	})
}

// --- GET /api/exports ------------------------------------------------------

type ExportFile struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	SizeStr  string `json:"size_str"`
	ModTime  string `json:"mod_time"`
	Category string `json:"category"` // "aggregated", "classified", "custom"
	Path     string `json:"path"`
}

func handleExports(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var files []ExportFile

	// Scan exports dir
	scanDir := func(dir, category string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, e := range entries {
			if e.IsDir() || (!strings.HasSuffix(e.Name(), ".csv") && !strings.HasSuffix(e.Name(), ".txt")) {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			files = append(files, ExportFile{
				Name:     e.Name(),
				Size:     info.Size(),
				SizeStr:  humanSize(info.Size()),
				ModTime:  info.ModTime().Format(time.RFC3339),
				Category: category,
				Path:     "/api/exports/download?file=" + e.Name() + "&cat=" + category,
			})
		}
	}

	scanDir(exportsDir, "aggregated")
	scanDir(classifiedDir, "classified")

	// Sort by mod time descending
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime > files[j].ModTime
	})

	jsonOK(w, files)
}

// --- GET /api/exports/download ---------------------------------------------

func handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fname := filepath.Base(r.URL.Query().Get("file"))
	cat := r.URL.Query().Get("cat")
	if fname == "" || fname == "." {
		http.Error(w, "file parameter required", 400)
		return
	}

	dir := exportsDir
	if cat == "classified" {
		dir = classifiedDir
	}
	fullPath := filepath.Join(dir, fname)

	// Security: verify the resolved path is within the expected directory
	absDir, _ := filepath.Abs(dir)
	absFile, _ := filepath.Abs(fullPath)
	if !strings.HasPrefix(absFile, absDir) {
		http.Error(w, "access denied", 403)
		return
	}

	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		http.Error(w, "file not found", 404)
		return
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fname))
	w.Header().Set("Content-Type", "text/csv")
	http.ServeFile(w, r, fullPath)
}

// --- POST /api/export/generate ---------------------------------------------

type GenerateRequest struct {
	Type string `json:"type"` // "aggregated", "aggregated_cup", "classified"
}

func handleGenerateExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req GenerateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid json: "+err.Error(), 400)
		return
	}

	switch req.Type {
	case "aggregated", "aggregated_cup", "classified":
	default:
		jsonError(w, "invalid type: must be aggregated, aggregated_cup, or classified", 400)
		return
	}

	job := newJob(req.Type)
	go runExportJob(job)

	jsonOK(w, job)
}

// --- GET /api/export/status/:id --------------------------------------------

func handleExportStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/api/export/status/")
	j := getJob(id)
	if j == nil {
		jsonError(w, "job not found", 404)
		return
	}

	jsonOK(w, j)
}

// ---------------------------------------------------------------------------
// Export Job Execution (background goroutines)
// ---------------------------------------------------------------------------

func runExportJob(job *ExportJob) {
	jobsMu.Lock()
	job.Status = "running"
	job.Message = "Discovering available years..."
	jobsMu.Unlock()

	years := discoverYears(filepath.Join(dataDir, "aiuti"))
	if len(years) == 0 {
		jobsMu.Lock()
		job.Status = "error"
		job.Message = "No data years found"
		jobsMu.Unlock()
		return
	}

	// Determine output directory and prefix based on export type
	var outDir, prefix string
	cupFilter := false

	switch job.Type {
	case "aggregated":
		outDir = exportsDir
		prefix = "aggregated"
	case "aggregated_cup":
		outDir = exportsDir
		prefix = "aggregated_cup"
		cupFilter = true
	case "classified":
		outDir = classifiedDir
		prefix = "classified_aiuti"
	}

	os.MkdirAll(outDir, 0755)

	for i, year := range years {
		jobsMu.Lock()
		job.Progress = float64(i) / float64(len(years))
		job.Message = fmt.Sprintf("Processing year %d (%d/%d)...", year, i+1, len(years))
		jobsMu.Unlock()

		outPath := filepath.Join(outDir, fmt.Sprintf("%s_%d.csv", prefix, year))

		var exportErr error
		if job.Type == "classified" {
			exportErr = exportClassifiedYear(dbPool, year, outPath)
		} else {
			exportErr = exportAggregatedYear(dbPool, year, outPath, cupFilter)
		}

		if exportErr != nil {
			log.Printf("Export error year %d: %v", year, exportErr)
			// Continue with next year
		}
	}

	jobsMu.Lock()
	job.Status = "done"
	job.Progress = 1.0
	job.Message = fmt.Sprintf("Completed: %d years exported", len(years))
	jobsMu.Unlock()
}

func exportAggregatedYear(db *sql.DB, year int, outPath string, cupFilter bool) error {
	aiutiGlob := filepath.Join(dataDir, "aiuti", fmt.Sprintf("ANNO=%d", year), "*.parquet")
	compGlob := filepath.Join(dataDir, "componenti", fmt.Sprintf("ANNO=%d", year), "*.parquet")
	strumGlob := filepath.Join(dataDir, "strumenti", fmt.Sprintf("ANNO=%d", year), "*.parquet")

	// Check aiuti dir exists
	aiutiDir := filepath.Join(dataDir, "aiuti", fmt.Sprintf("ANNO=%d", year))
	if _, err := os.Stat(aiutiDir); os.IsNotExist(err) {
		return nil
	}

	// Check if componenti / strumenti exist for this year
	compDir := filepath.Join(dataDir, "componenti", fmt.Sprintf("ANNO=%d", year))
	strumDir := filepath.Join(dataDir, "strumenti", fmt.Sprintf("ANNO=%d", year))
	hasComp := dirExists(compDir)
	hasStrum := dirExists(strumDir)

	// Build the aggregation query dynamically
	var joins, strumAggs string

	if hasComp {
		joins += fmt.Sprintf(`
LEFT JOIN read_parquet('%s', union_by_name=true) c
  ON a.CAR = c.CAR_AIUTO AND a.COR = c.COR_AIUTO`, compGlob)
	}
	if hasStrum && hasComp {
		joins += fmt.Sprintf(`
LEFT JOIN read_parquet('%s', union_by_name=true) s
  ON c.ID_COMPONENTE_AIUTO = s.ID_COMPONENTE_AIUTO`, strumGlob)
		strumAggs = `,
  COALESCE(SUM(s.IMPORTO_NOMINALE), 0) AS IMPORTO_NOMINALE_TOTALE,
  COALESCE(SUM(s.ELEMENTO_DI_AIUTO), 0) AS ELEMENTO_DI_AIUTO_TOTALE,
  COUNT(DISTINCT c.ID_COMPONENTE_AIUTO) AS NUM_COMPONENTI,
  COUNT(s.COD_STRUMENTO) AS NUM_STRUMENTI,
  STRING_AGG(DISTINCT s.COD_STRUMENTO, '|') AS COD_STRUMENTI,
  STRING_AGG(DISTINCT c.SETTORE_ATTIVITA, '|') AS SETTORI_ATTIVITA,
  STRING_AGG(DISTINCT c.DES_OBIETTIVO, '|') AS OBIETTIVO`
	} else if hasComp {
		strumAggs = `,
  0 AS IMPORTO_NOMINALE_TOTALE,
  0 AS ELEMENTO_DI_AIUTO_TOTALE,
  COUNT(DISTINCT c.ID_COMPONENTE_AIUTO) AS NUM_COMPONENTI,
  0 AS NUM_STRUMENTI,
  NULL AS COD_STRUMENTI,
  STRING_AGG(DISTINCT c.SETTORE_ATTIVITA, '|') AS SETTORI_ATTIVITA,
  STRING_AGG(DISTINCT c.DES_OBIETTIVO, '|') AS OBIETTIVO`
	} else {
		strumAggs = `,
  0 AS IMPORTO_NOMINALE_TOTALE,
  0 AS ELEMENTO_DI_AIUTO_TOTALE,
  0 AS NUM_COMPONENTI,
  0 AS NUM_STRUMENTI,
  NULL AS COD_STRUMENTI,
  NULL AS SETTORI_ATTIVITA,
  NULL AS OBIETTIVO`
	}

	cupWhere := ""
	if cupFilter {
		cupWhere = `
HAVING MAX(a.CUP) IS NOT NULL
  AND MAX(a.CUP) != ''
  AND MAX(a.CUP) != 'n.d.'`
	}

	query := fmt.Sprintf(`
COPY (
  SELECT
    a.CAR, a.TITOLO_MISURA, a.DES_TIPO_MISURA, a.TITOLO_PROGETTO,
    a.DESCRIZIONE_PROGETTO, a.DATA_CONCESSIONE, a.CUP,
    a.DENOMINAZIONE_BENEFICIARIO, a.CODICE_FISCALE_BENEFICIARIO,
    a.DES_TIPO_BENEFICIARIO, a.REGIONE_BENEFICIARIO,
    a.FILE_SOURCE, a.COR, %d AS ANNO
    %s
  FROM read_parquet('%s', union_by_name=true) a
  %s
  GROUP BY a.CAR, a.TITOLO_MISURA, a.DES_TIPO_MISURA, a.TITOLO_PROGETTO,
    a.DESCRIZIONE_PROGETTO, a.DATA_CONCESSIONE, a.CUP,
    a.DENOMINAZIONE_BENEFICIARIO, a.CODICE_FISCALE_BENEFICIARIO,
    a.DES_TIPO_BENEFICIARIO, a.REGIONE_BENEFICIARIO,
    a.FILE_SOURCE, a.COR
  %s
) TO '%s' (HEADER, DELIMITER ',')`,
		year, strumAggs, aiutiGlob, joins, cupWhere, outPath)

	_, err := db.Exec(query)
	return err
}

func exportClassifiedYear(db *sql.DB, year int, outPath string) error {
	aiutiGlob := filepath.Join(dataDir, "aiuti", fmt.Sprintf("ANNO=%d", year), "*.parquet")
	compGlob := filepath.Join(dataDir, "componenti", fmt.Sprintf("ANNO=%d", year), "*.parquet")
	strumGlob := filepath.Join(dataDir, "strumenti", fmt.Sprintf("ANNO=%d", year), "*.parquet")
	cacheGlob := filepath.Join(cacheDir, "classification_cache.parquet")

	// Check cache exists
	if _, err := os.Stat(cacheGlob); os.IsNotExist(err) {
		return fmt.Errorf("classification cache not found at %s", cacheGlob)
	}

	aiutiDir := filepath.Join(dataDir, "aiuti", fmt.Sprintf("ANNO=%d", year))
	if _, err := os.Stat(aiutiDir); os.IsNotExist(err) {
		return nil
	}

	compDir := filepath.Join(dataDir, "componenti", fmt.Sprintf("ANNO=%d", year))
	strumDir := filepath.Join(dataDir, "strumenti", fmt.Sprintf("ANNO=%d", year))
	hasComp := dirExists(compDir)
	hasStrum := dirExists(strumDir)

	var joins, strumAggs string

	if hasComp {
		joins += fmt.Sprintf(`
LEFT JOIN read_parquet('%s', union_by_name=true) c
  ON a.CAR = c.CAR_AIUTO AND a.COR = c.COR_AIUTO`, compGlob)
	}
	if hasStrum && hasComp {
		joins += fmt.Sprintf(`
LEFT JOIN read_parquet('%s', union_by_name=true) s
  ON c.ID_COMPONENTE_AIUTO = s.ID_COMPONENTE_AIUTO`, strumGlob)
		strumAggs = `,
  COALESCE(SUM(s.IMPORTO_NOMINALE), 0) AS IMPORTO_NOMINALE_TOTALE,
  COALESCE(SUM(s.ELEMENTO_DI_AIUTO), 0) AS ELEMENTO_DI_AIUTO_TOTALE,
  COUNT(DISTINCT c.ID_COMPONENTE_AIUTO) AS NUM_COMPONENTI,
  COUNT(s.COD_STRUMENTO) AS NUM_STRUMENTI,
  STRING_AGG(DISTINCT s.COD_STRUMENTO, '|') AS COD_STRUMENTI,
  STRING_AGG(DISTINCT c.SETTORE_ATTIVITA, '|') AS SETTORI_ATTIVITA,
  STRING_AGG(DISTINCT c.DES_OBIETTIVO, '|') AS OBIETTIVO`
	} else if hasComp {
		strumAggs = `,
  0 AS IMPORTO_NOMINALE_TOTALE,
  0 AS ELEMENTO_DI_AIUTO_TOTALE,
  COUNT(DISTINCT c.ID_COMPONENTE_AIUTO) AS NUM_COMPONENTI,
  0 AS NUM_STRUMENTI,
  NULL AS COD_STRUMENTI,
  STRING_AGG(DISTINCT c.SETTORE_ATTIVITA, '|') AS SETTORI_ATTIVITA,
  STRING_AGG(DISTINCT c.DES_OBIETTIVO, '|') AS OBIETTIVO`
	} else {
		strumAggs = `,
  0 AS IMPORTO_NOMINALE_TOTALE,
  0 AS ELEMENTO_DI_AIUTO_TOTALE,
  0 AS NUM_COMPONENTI,
  0 AS NUM_STRUMENTI,
  NULL AS COD_STRUMENTI,
  NULL AS SETTORI_ATTIVITA,
  NULL AS OBIETTIVO`
	}

	// Add classification cache join
	joins += fmt.Sprintf(`
LEFT JOIN read_parquet('%s') cl
  ON a.DESCRIZIONE_PROGETTO = cl.DESCRIZIONE_PROGETTO`, cacheGlob)

	query := fmt.Sprintf(`
COPY (
  SELECT
    a.CAR, a.TITOLO_MISURA, a.DES_TIPO_MISURA, a.TITOLO_PROGETTO,
    a.DESCRIZIONE_PROGETTO, a.DATA_CONCESSIONE, a.CUP,
    a.DENOMINAZIONE_BENEFICIARIO, a.CODICE_FISCALE_BENEFICIARIO,
    a.DES_TIPO_BENEFICIARIO, a.REGIONE_BENEFICIARIO,
    a.FILE_SOURCE, a.COR, %d AS ANNO
    %s,
    COALESCE(MAX(cl.CLASSIFICAZIONE), 'UNKNOWN') AS CLASSIFICAZIONE,
    COALESCE(MAX(cl.CLASSIFICAZIONE_CONFIDENZA), 0.0) AS CLASSIFICAZIONE_CONFIDENZA
  FROM read_parquet('%s', union_by_name=true) a
  %s
  GROUP BY a.CAR, a.TITOLO_MISURA, a.DES_TIPO_MISURA, a.TITOLO_PROGETTO,
    a.DESCRIZIONE_PROGETTO, a.DATA_CONCESSIONE, a.CUP,
    a.DENOMINAZIONE_BENEFICIARIO, a.CODICE_FISCALE_BENEFICIARIO,
    a.DES_TIPO_BENEFICIARIO, a.REGIONE_BENEFICIARIO,
    a.FILE_SOURCE, a.COR
) TO '%s' (HEADER, DELIMITER ',')`,
		year, strumAggs, aiutiGlob, joins, outPath)

	_, err := db.Exec(query)
	return err
}

// ---------------------------------------------------------------------------
// Streaming CSV export (for /api/export/csv/stream)
// ---------------------------------------------------------------------------

func handleExportCSVStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ExportCSVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid json: "+err.Error(), 400)
		return
	}

	if strings.TrimSpace(req.SQL) == "" {
		jsonError(w, "sql is required", 400)
		return
	}

	fname := req.Filename
	if fname == "" {
		fname = fmt.Sprintf("query_export_%d.csv", time.Now().Unix())
	}
	fname = filepath.Base(fname)
	if !strings.HasSuffix(fname, ".csv") {
		fname += ".csv"
	}

	rows, err := dbPool.Query(req.SQL)
	if err != nil {
		jsonError(w, "query error: "+err.Error(), 400)
		return
	}
	defer rows.Close()

	colTypes, _ := rows.ColumnTypes()
	colNames := make([]string, len(colTypes))
	for i, ct := range colTypes {
		colNames[i] = ct.Name()
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fname))
	w.Header().Set("Content-Type", "text/csv")

	csvW := csv.NewWriter(w)
	csvW.Write(colNames) // header

	scanArgs := make([]interface{}, len(colNames))
	scanVals := make([]interface{}, len(colNames))
	for i := range scanVals {
		scanArgs[i] = &scanVals[i]
	}

	row := make([]string, len(colNames))
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			continue
		}
		for i, v := range scanVals {
			row[i] = fmt.Sprintf("%v", formatValue(v))
		}
		csvW.Write(row)
	}
	csvW.Flush()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func formatValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	default:
		return val
	}
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

func humanSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func jsonOK(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ---------------------------------------------------------------------------
// CORS Middleware
// ---------------------------------------------------------------------------

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	// Initialize persistent DuckDB connection pool
	initDuckDB()
	defer dbPool.Close()

	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/api/tables", handleTables)
	mux.HandleFunc("/api/schema/", handleSchema)
	mux.HandleFunc("/api/templates", handleTemplates)
	mux.HandleFunc("/api/query", handleQuery)
	mux.HandleFunc("/api/export/csv", handleExportCSV)
	mux.HandleFunc("/api/export/csv/stream", handleExportCSVStream)
	mux.HandleFunc("/api/exports", handleExports)
	mux.HandleFunc("/api/exports/download", handleDownload)
	mux.HandleFunc("/api/export/generate", handleGenerateExport)
	mux.HandleFunc("/api/export/status/", handleExportStatus)

	// Static files (frontend)
	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/", fs)

	handler := corsMiddleware(mux)

	log.Printf("🚀 Open Data Chunker Web — listening on :%s", listenPort)
	log.Printf("   Data dir:       %s", dataDir)
	log.Printf("   Exports dir:    %s", exportsDir)
	log.Printf("   Classified dir: %s", classifiedDir)
	log.Printf("   Cache dir:      %s", cacheDir)

	if err := http.ListenAndServe(":"+listenPort, handler); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
