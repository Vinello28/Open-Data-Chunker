/* ==========================================================================
   Open Data Chunker — Frontend Application Logic
   ========================================================================== */

(function () {
    'use strict';

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------
    let currentPage = 'query';
    let tablesData = [];
    let activeFilter = 'all';

    // -----------------------------------------------------------------------
    // Navigation
    // -----------------------------------------------------------------------
    document.querySelectorAll('.nav-item').forEach(btn => {
        btn.addEventListener('click', () => {
            const page = btn.dataset.page;
            switchPage(page);
        });
    });

    function switchPage(page) {
        currentPage = page;
        document.querySelectorAll('.nav-item').forEach(b => b.classList.remove('active'));
        document.querySelector(`.nav-item[data-page="${page}"]`).classList.add('active');
        document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
        document.getElementById(`page-${page}`).classList.add('active');

        if (page === 'exports') {
            loadExportFiles();
        }
    }

    // -----------------------------------------------------------------------
    // Init: load tables + schema
    // -----------------------------------------------------------------------
    async function init() {
        try {
            const res = await fetch('/api/tables');
            tablesData = await res.json();
            updateSidebarStats(tablesData);
            loadSchemas(tablesData);
            loadTemplates();
        } catch (e) {
            console.error('Init error:', e);
        }
    }

    function updateSidebarStats(tables) {
        const totalRecords = tables.reduce((sum, t) => sum + (t.count || 0), 0);
        document.querySelector('#stat-tables .stat-value').textContent = tables.length;
        document.querySelector('#stat-records .stat-value').textContent = formatNumber(totalRecords);
    }

    async function loadSchemas(tables) {
        const container = document.getElementById('schema-panels');
        container.innerHTML = '';

        for (const t of tables) {
            try {
                const res = await fetch(`/api/schema/${t.name}`);
                const cols = await res.json();

                const panel = document.createElement('div');
                panel.className = 'schema-panel';
                panel.innerHTML = `
                    <div class="schema-panel-title">${t.name}</div>
                    <div class="schema-cols">
                        ${cols.map(c => `
                            <div class="schema-col">
                                <span class="schema-col-name">${c.name}</span>
                                <span class="schema-col-type">${c.type}</span>
                            </div>
                        `).join('')}
                    </div>
                `;
                container.appendChild(panel);
            } catch (e) {
                console.error(`Schema load error for ${t.name}:`, e);
            }
        }
    }

    // Schema toggle
    document.getElementById('schema-bar-header')?.addEventListener('click', toggleSchema);
    document.getElementById('schema-toggle')?.addEventListener('click', toggleSchema);
    document.querySelector('.schema-bar-header')?.addEventListener('click', toggleSchema);

    function toggleSchema() {
        const panels = document.getElementById('schema-panels');
        const toggle = document.getElementById('schema-toggle');
        panels.classList.toggle('open');
        toggle.classList.toggle('open');
    }

    // -----------------------------------------------------------------------
    // Templates
    // -----------------------------------------------------------------------
    async function loadTemplates() {
        try {
            const res = await fetch('/api/templates');
            const templates = await res.json();
            const list = document.getElementById('templates-list');
            list.innerHTML = '';

            templates.forEach(t => {
                const item = document.createElement('div');
                item.className = 'template-item';
                item.innerHTML = `
                    <div class="template-name">${t.name}</div>
                    <div class="template-desc">${t.description}</div>
                `;
                item.addEventListener('click', () => {
                    document.getElementById('sql-editor').value = t.sql;
                    updateLineNumbers();
                    toast('Query caricata', 'info');
                });
                list.appendChild(item);
            });
        } catch (e) {
            console.error('Templates load error:', e);
        }
    }

    // Templates toggle
    document.querySelector('.templates-header')?.addEventListener('click', toggleTemplates);

    function toggleTemplates() {
        const list = document.getElementById('templates-list');
        const toggle = document.getElementById('templates-toggle');
        list.classList.toggle('hidden');
        toggle.classList.toggle('open');
    }

    // -----------------------------------------------------------------------
    // SQL Editor
    // -----------------------------------------------------------------------
    const editor = document.getElementById('sql-editor');
    const lineNumbers = document.getElementById('line-numbers');

    editor.addEventListener('input', updateLineNumbers);
    editor.addEventListener('scroll', () => {
        lineNumbers.scrollTop = editor.scrollTop;
    });

    editor.addEventListener('keydown', (e) => {
        // Ctrl+Enter → Run query
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault();
            runQuery();
            return;
        }

        // Tab → insert 2 spaces
        if (e.key === 'Tab') {
            e.preventDefault();
            const start = editor.selectionStart;
            const end = editor.selectionEnd;
            editor.value = editor.value.substring(0, start) + '  ' + editor.value.substring(end);
            editor.selectionStart = editor.selectionEnd = start + 2;
            updateLineNumbers();
        }
    });

    function updateLineNumbers() {
        const lines = editor.value.split('\n').length;
        const nums = [];
        for (let i = 1; i <= lines; i++) nums.push(i);
        lineNumbers.textContent = nums.join('\n');
    }

    // -----------------------------------------------------------------------
    // Query Execution
    // -----------------------------------------------------------------------
    document.getElementById('btn-run').addEventListener('click', runQuery);

    async function runQuery() {
        const sql = editor.value.trim();
        if (!sql) {
            toast('Scrivi una query SQL prima di eseguire', 'error');
            return;
        }

        const statusEl = document.getElementById('editor-status');
        const resultsContainer = document.getElementById('results-container');
        const resultsInfo = document.getElementById('results-info');
        const btnRun = document.getElementById('btn-run');

        btnRun.disabled = true;
        statusEl.textContent = 'Esecuzione in corso...';
        resultsContainer.innerHTML = '<div class="loading-overlay"><div class="spinner"></div><span>Elaborazione query...</span></div>';

        try {
            const res = await fetch('/api/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql, limit: 1000 })
            });

            const data = await res.json();

            if (data.error) {
                statusEl.textContent = 'Errore';
                resultsContainer.innerHTML = `<div class="results-placeholder"><span class="placeholder-icon">✕</span><p style="color: var(--error);">${escapeHtml(data.error)}</p></div>`;
                toast('Errore query: ' + data.error, 'error');
                return;
            }

            statusEl.textContent = `${data.row_count} righe · ${data.duration}`;

            let infoText = `${data.row_count} righe · ${data.duration}`;
            if (data.truncated) {
                infoText += ' <span class="truncated-badge">⚠ Troncato a 1000 righe (preview)</span>';
            }
            resultsInfo.innerHTML = infoText;

            renderTable(data, resultsContainer);

        } catch (e) {
            statusEl.textContent = 'Errore di rete';
            resultsContainer.innerHTML = `<div class="results-placeholder"><span class="placeholder-icon">✕</span><p style="color: var(--error);">Errore di connessione al server</p></div>`;
            toast('Errore di rete: ' + e.message, 'error');
        } finally {
            btnRun.disabled = false;
        }
    }

    function renderTable(data, container) {
        if (!data.columns || data.columns.length === 0) {
            container.innerHTML = '<div class="results-placeholder"><p>Nessun risultato</p></div>';
            return;
        }

        const table = document.createElement('table');
        table.className = 'results-table';

        // Header
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        data.columns.forEach(col => {
            const th = document.createElement('th');
            th.textContent = col;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        // Body
        const tbody = document.createElement('tbody');
        const rows = data.rows || [];
        rows.forEach(row => {
            const tr = document.createElement('tr');
            row.forEach(val => {
                const td = document.createElement('td');
                if (val === null || val === undefined) {
                    td.textContent = 'NULL';
                    td.className = 'null-value';
                } else if (typeof val === 'number') {
                    td.textContent = formatNumber(val);
                    td.className = 'number-value';
                } else {
                    td.textContent = String(val);
                }
                td.title = String(val);
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);

        container.innerHTML = '';
        container.appendChild(table);
    }

    // -----------------------------------------------------------------------
    // CSV Export from Query
    // -----------------------------------------------------------------------
    document.getElementById('btn-export-csv').addEventListener('click', () => {
        const sql = editor.value.trim();
        if (!sql) {
            toast('Scrivi una query prima di esportare', 'error');
            return;
        }
        showExportModal();
    });

    document.getElementById('btn-export-stream').addEventListener('click', () => {
        const sql = editor.value.trim();
        if (!sql) {
            toast('Scrivi una query prima di esportare', 'error');
            return;
        }
        streamDownloadCSV(sql);
    });

    function showExportModal() {
        document.getElementById('export-modal').classList.remove('hidden');
        document.getElementById('export-filename').value = `query_export_${Date.now()}.csv`;
        document.getElementById('export-filename').focus();
    }

    function hideExportModal() {
        document.getElementById('export-modal').classList.add('hidden');
    }

    document.getElementById('modal-close').addEventListener('click', hideExportModal);
    document.getElementById('modal-cancel').addEventListener('click', hideExportModal);

    document.getElementById('modal-confirm').addEventListener('click', async () => {
        const sql = editor.value.trim();
        const filename = document.getElementById('export-filename').value.trim();
        hideExportModal();

        toast('Esportazione in corso...', 'info');

        try {
            const res = await fetch('/api/export/csv', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql, filename })
            });

            const data = await res.json();
            if (data.error) {
                toast('Errore export: ' + data.error, 'error');
                return;
            }

            toast(`Export completato: ${data.filename} (${humanSize(data.size)}) in ${data.duration}`, 'success');

            // Auto-download
            window.open(data.path, '_blank');
        } catch (e) {
            toast('Errore di rete: ' + e.message, 'error');
        }
    });

    // Streaming CSV download — sends query results directly as response body
    async function streamDownloadCSV(sql) {
        toast('Download CSV in corso (streaming)...', 'info');

        try {
            const res = await fetch('/api/export/csv/stream', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql, filename: `export_${Date.now()}.csv` })
            });

            if (!res.ok) {
                const err = await res.json();
                toast('Errore: ' + (err.error || 'unknown'), 'error');
                return;
            }

            // Get filename from Content-Disposition header
            const disposition = res.headers.get('Content-Disposition') || '';
            const filenameMatch = disposition.match(/filename="?([^"]+)"?/);
            const fname = filenameMatch ? filenameMatch[1] : `export_${Date.now()}.csv`;

            const blob = await res.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = fname;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);

            toast(`Download completato: ${fname}`, 'success');
        } catch (e) {
            toast('Errore download: ' + e.message, 'error');
        }
    }

    // -----------------------------------------------------------------------
    // Export Generation (Aggregated / Classified)
    // -----------------------------------------------------------------------
    document.querySelectorAll('.btn-generate').forEach(btn => {
        btn.addEventListener('click', () => {
            startGenerateExport(btn.dataset.type);
        });
    });

    async function startGenerateExport(type) {
        const btn = document.querySelector(`.btn-generate[data-type="${type}"]`);
        const progressContainer = document.getElementById(`progress-${type}`);
        const progressFill = document.getElementById(`progress-fill-${type}`);
        const progressText = document.getElementById(`progress-text-${type}`);

        btn.disabled = true;
        progressContainer.classList.remove('hidden');
        progressFill.style.width = '0%';
        progressText.textContent = 'Avvio...';

        try {
            const res = await fetch('/api/export/generate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ type })
            });

            const job = await res.json();
            if (job.error) {
                toast('Errore: ' + job.error, 'error');
                btn.disabled = false;
                progressContainer.classList.add('hidden');
                return;
            }

            // Poll for progress
            pollExportJob(job.id, type, btn, progressContainer, progressFill, progressText);

        } catch (e) {
            toast('Errore di rete: ' + e.message, 'error');
            btn.disabled = false;
            progressContainer.classList.add('hidden');
        }
    }

    function pollExportJob(jobId, type, btn, container, fill, text) {
        const interval = setInterval(async () => {
            try {
                const res = await fetch(`/api/export/status/${jobId}`);
                const job = await res.json();

                const pct = Math.round((job.progress || 0) * 100);
                fill.style.width = `${pct}%`;
                text.textContent = job.message || `${pct}%`;

                if (job.status === 'done') {
                    clearInterval(interval);
                    fill.style.width = '100%';
                    text.textContent = '✓ Completato';
                    btn.disabled = false;
                    toast(`Export ${type} completato!`, 'success');

                    // Refresh file list if on exports page
                    if (currentPage === 'exports') {
                        setTimeout(() => loadExportFiles(), 500);
                    }

                    // Hide progress after a delay
                    setTimeout(() => {
                        container.classList.add('hidden');
                    }, 3000);
                }

                if (job.status === 'error') {
                    clearInterval(interval);
                    text.textContent = '✕ Errore';
                    btn.disabled = false;
                    toast(`Errore export: ${job.message}`, 'error');

                    setTimeout(() => {
                        container.classList.add('hidden');
                    }, 3000);
                }
            } catch (e) {
                clearInterval(interval);
                btn.disabled = false;
                container.classList.add('hidden');
            }
        }, 1000);
    }

    // -----------------------------------------------------------------------
    // Export File Listing
    // -----------------------------------------------------------------------
    document.getElementById('btn-refresh-files').addEventListener('click', loadExportFiles);

    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            activeFilter = btn.dataset.filter;
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            loadExportFiles();
        });
    });

    async function loadExportFiles() {
        const container = document.getElementById('files-list');
        container.innerHTML = '<div class="files-placeholder"><div class="spinner"></div><p>Caricamento...</p></div>';

        try {
            const res = await fetch('/api/exports');
            let files = await res.json();

            if (!files || files.length === 0) {
                container.innerHTML = '<div class="files-placeholder"><span class="placeholder-icon">📁</span><p>Nessun file di export trovato. Genera un export dalla sezione sopra.</p></div>';
                return;
            }

            // Apply filter
            if (activeFilter !== 'all') {
                files = files.filter(f => f.category === activeFilter);
            }

            if (files.length === 0) {
                container.innerHTML = '<div class="files-placeholder"><span class="placeholder-icon">📁</span><p>Nessun file per questa categoria.</p></div>';
                return;
            }

            container.innerHTML = '';
            files.forEach(f => {
                const row = document.createElement('div');
                row.className = 'file-row';
                row.dataset.category = f.category;

                const iconMap = {
                    aggregated: '📊',
                    classified: '🤖',
                    custom: '📄'
                };

                row.innerHTML = `
                    <div class="file-info">
                        <span class="file-icon">${iconMap[f.category] || '📄'}</span>
                        <div class="file-details">
                            <div class="file-name">${escapeHtml(f.name)}</div>
                            <div class="file-meta">
                                <span>${f.size_str}</span>
                                <span>${formatDate(f.mod_time)}</span>
                                <span class="file-category-badge ${f.category}">${f.category}</span>
                            </div>
                        </div>
                    </div>
                    <div class="file-actions">
                        <a href="${f.path}" class="btn btn-primary btn-sm" download>
                            <span class="btn-icon">↓</span> Download
                        </a>
                    </div>
                `;

                container.appendChild(row);
            });

        } catch (e) {
            container.innerHTML = '<div class="files-placeholder"><span class="placeholder-icon">✕</span><p>Errore nel caricamento dei file</p></div>';
            console.error('Files load error:', e);
        }
    }

    // -----------------------------------------------------------------------
    // Toast Notifications
    // -----------------------------------------------------------------------
    function toast(message, type = 'info') {
        const container = document.getElementById('toast-container');
        const el = document.createElement('div');
        el.className = `toast ${type}`;

        const icons = { success: '✓', error: '✕', info: 'ℹ' };
        el.innerHTML = `<span>${icons[type] || 'ℹ'}</span><span>${escapeHtml(message)}</span>`;

        container.appendChild(el);

        setTimeout(() => {
            el.style.opacity = '0';
            el.style.transform = 'translateY(8px)';
            el.style.transition = 'all 300ms ease';
            setTimeout(() => el.remove(), 300);
        }, 4000);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------
    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function formatNumber(n) {
        if (typeof n !== 'number') return String(n);
        return n.toLocaleString('it-IT');
    }

    function humanSize(bytes) {
        if (typeof bytes !== 'number' || bytes === 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
    }

    function formatDate(isoStr) {
        if (!isoStr) return '';
        try {
            const d = new Date(isoStr);
            return d.toLocaleDateString('it-IT', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        } catch {
            return isoStr;
        }
    }

    // -----------------------------------------------------------------------
    // Keyboard shortcut overlay (ESC to close modal)
    // -----------------------------------------------------------------------
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            hideExportModal();
        }
    });

    // -----------------------------------------------------------------------
    // Bootstrap
    // -----------------------------------------------------------------------
    init();
    updateLineNumbers();

})();
