Per gestire efficientemente i milioni di progetti di agevolazione presenti nel dataset, è stato introdotto un approccio scalabile a doppia cascata "Caching-First", supportato da due modelli AI (encoder-only) fine-tuned sugli specifici task di classificazione. Dapprima, tramite elaborazione in DuckDB, viene estratto le descrizioni progettuali uniche (DESCRIZIONE_PROGETTO). Tale base documentale viene sottomessa all'applicativo server su cui risiede il modello. 

Entrambi i modelli sono stati addestrati su dataset distillati da "Ministral 3 4B" e "GPT-OSS-20B", con dataset di test etichettato prevalentemente a mano, per garantire la qualità dei risultati. 

L'inferenza che permette di arrivare al dataset finale prevede due step, uno per la costruzione della cache binaria ed uno per la multiclasse.

- Fase 1 (Classificazione Binaria): viene utilizzato il modello "bert-base-italian-xxl-cased", che valuta la descrizione per distinguere univocamente se il progetto sovvenzionato implica, sviluppa o impiega esplicitamente tecnologie di Intelligenza Artificiale rispetto a progetti di natura ordinaria/diversa. Solamente i progetti etichettati come "AI" passano alla fase successiva.

- Fase 2 (Classificazione Multiclasse): i testi "AI-related" vengono nuovamente processati dal modello "xlm-roberta-large" per determinarne lo specifico ambito applicativo dell'IA (es. Computer Vision, Analisi Dati, Robotica, ecc.), scartando a priori ogni computazione sui progetti tradizionali. 

Entrambi i passaggi generano file di cache intermedi Parquet. Al termine della pipeline, il layer di esportazione aggrega i dati annuali mediante Polars, eseguendo un Double Left Join con le due cache: in questo modo i progetti ordinari manterranno colonne "NULL/UNKNOWN" per l'ambito applicativo, restituendo un tracciato CSV inestimabile per completezza formale ma computato in una frazione del potenziale tempo rispetto ai metodi convenzionali.

-----

INGLESE

To efficiently manage the millions of subsidy projects in the dataset, a scalable “Caching-First” two-stage approach was introduced, supported by two AI models (encoder-only) fine-tuned for specific classification tasks. First, through processing in DuckDB, the unique project descriptions are extracted. This document base is submitted to the server application where the model resides. 

Both models were trained on datasets distilled from “Ministral 3 4B” and “GPT-OSS-20B,” with test datasets primarily labeled by hand to ensure the quality of the results. 

The inference process leading to the final dataset involves two steps: one for building the binary cache and one for multi-class classification.

- Phase 1 (Binary Classification): The “bert-base-italian-xxl-cased” model is used to evaluate the description and unambiguously determine whether the funded project explicitly involves, develops, or employs Artificial Intelligence technologies compared to projects of an ordinary or different nature. Only projects labeled as “AI” proceed to the next phase.

- Phase 2 (Multiclass Classification): “AI-related” texts are processed again by the “xlm-roberta-large” model to determine the specific AI application domain (e.g., Computer Vision, Data Analysis, Robotics, etc.), discarding any processing of traditional projects.

Both steps generate intermediate Parquet cache files. At the end of the pipeline, the export layer aggregates the annual data using Polars, performing a Double Left Join with the two caches: in this way, ordinary projects will retain “NULL/UNKNOWN” columns for the application domain, returning a CSV track that is invaluable for formal completeness but computed in a fraction of the time compared to conventional methods.