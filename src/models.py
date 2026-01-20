import polars as pl
import pyarrow as pa

# Schema per la tabella principale AIUTI
SCHEMA_AIUTI = pa.schema([
    ('CAR', pa.string()),
    ('TITOLO_MISURA', pa.string()),
    ('DES_TIPO_MISURA', pa.string()),
    ('BASE_GIURIDICA_NAZIONALE', pa.string()),
    ('CODICE_FISCALE_BENEFICIARIO', pa.string()),
    ('DENOMINAZIONE_BENEFICIARIO', pa.string()),
    ('TITOLO_PROGETTO', pa.string()),
    ('COR', pa.string()),
    ('DATA_CONCESSIONE', pa.string()),  # Convertito poi in date
    ('ANNO', pa.int32()),  # Partizione
    ('FILE_SOURCE', pa.string()) # Tracciabilità
])

# Schema per COMPONENTI_AIUTO
SCHEMA_COMPONENTI = pa.schema([
    ('ID_COMPONENTE_AIUTO', pa.string()),
    ('CAR_AIUTO', pa.string()), # FK verso AIUTI (usando CAR come link logico, anche se non univoco tra bandi diversi, meglio usare combinazione CAR+COR o un ID sintetico)
    ('COR_AIUTO', pa.string()), # FK aggiuntiva
    ('COD_PROCEDIMENTO', pa.string()),
    ('DES_PROCEDIMENTO', pa.string()),
    ('COD_REGOLAMENTO', pa.string()),
    ('DES_REGOLAMENTO', pa.string()),
    ('COD_OBIETTIVO', pa.string()),
    ('DES_OBIETTIVO', pa.string()),
    ('SETTORE_ATTIVITA', pa.string()),
    ('ANNO', pa.int32())
])

# Schema per STRUMENTI_AIUTO
SCHEMA_STRUMENTI = pa.schema([
    ('ID_COMPONENTE_AIUTO', pa.string()), # FK verso COMPONENTI
    ('COD_STRUMENTO', pa.string()),
    ('DES_STRUMENTO', pa.string()),
    ('ELEMENTO_DI_AIUTO', pa.float64()),
    ('IMPORTO_NOMINALE', pa.float64()),
    ('ANNO', pa.int32())
])
