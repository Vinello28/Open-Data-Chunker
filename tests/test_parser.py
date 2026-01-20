import pytest
import os
from src.parser import process_file
import pyarrow.parquet as pq
from pathlib import Path

# Un mock di XML per il test
XML_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<LISTA_AIUTI xmlns="http://www.rna.it/RNA_aiuto/schema">
    <AIUTO>
        <CAR>12345</CAR>
        <TITOLO_MISURA>Misura Test</TITOLO_MISURA>
        <DATA_CONCESSIONE>2022-01-01</DATA_CONCESSIONE>
        <COMPONENTI_AIUTO>
            <COMPONENTE_AIUTO>
                <ID_COMPONENTE_AIUTO>999</ID_COMPONENTE_AIUTO>
                <STRUMENTI_AIUTO>
                    <STRUMENTO_AIUTO>
                        <IMPORTO_NOMINALE>1000.00</IMPORTO_NOMINALE>
                    </STRUMENTO_AIUTO>
                </STRUMENTI_AIUTO>
            </COMPONENTE_AIUTO>
        </COMPONENTI_AIUTO>
    </AIUTO>
</LISTA_AIUTI>
"""

@pytest.fixture
def sample_xml(tmp_path):
    p = tmp_path / "test.xml"
    p.write_text(XML_CONTENT)
    return str(p)

def test_process_file(sample_xml, tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    stats = process_file(sample_xml, str(output_dir))
    
    assert stats["aiuti"] == 1
    assert stats["componenti"] == 1
    assert stats["strumenti"] == 1
    
    # Check if parquet files are created
    assert (output_dir / "aiuti").exists()
    assert (output_dir / "componenti").exists()
    assert (output_dir / "strumenti").exists()
    
    # Check partition year=2022
    assert (output_dir / "aiuti" / "ANNO=2022").exists()
