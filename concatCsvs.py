# concat_csvs.py
from pathlib import Path
from typing import Iterable
import pandas as pd
import sys

DEFAULT_INPUT = "csv_input"
DEFAULT_OUTPUT = "output/merged_all.csv"
GLOB_PATTERN = "*.csv"
CHUNKSIZE = 200_000

EXPECTED_COLUMNS = [
    "lead_id",
    "created_time",
    "created_date",
    "ad_id",
    "campaign_id",
    "account_id",
    "form_id",
    "form_name",
    "test_lead",
    "Nombre",
    "Apellidos",
    "Email",
    "Teléfono",
    "País/región",
    "lead_type",
]

OUT_COLUMNS = EXPECTED_COLUMNS + ["archivoOrigen"]


def processChunks(reader: Iterable[pd.DataFrame], filename: str, output_file: str, header_written_ref: dict) -> int:
    rows_written = 0
    for chunk in reader:
        chunk = chunk.astype(str, copy=False)
        chunk = chunk.reindex(columns=EXPECTED_COLUMNS)
        chunk = chunk.fillna("")
        chunk["archivoOrigen"] = filename
        chunk = chunk.reindex(columns=OUT_COLUMNS)
        chunk.to_csv(output_file, mode="a",
                     header=not header_written_ref["val"], index=False)
        header_written_ref["val"] = True
        rows_written += len(chunk)
    return rows_written


def concatCSVFolder(input_folder: str, output_file: str, glob_pattern: str = GLOB_PATTERN, chunksize: int = CHUNKSIZE) -> int:
    p = Path(input_folder)
    files = sorted(p.glob(glob_pattern))
    if not files:
        print(
            f"No se encontraron archivos en {input_folder} con patrón {glob_pattern}", file=sys.stderr)
        return 1

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    header_written = {"val": False}
    total_rows = 0

    for f in files:
        try:
            reader = pd.read_csv(
                f, dtype=str, chunksize=chunksize, low_memory=False, na_filter=False)
            total_rows += processChunks(reader,
                                        f.name, output_file, header_written)
        except pd.errors.EmptyDataError:
            print(f"Archivo vacío: {f}", file=sys.stderr)
        except UnicodeDecodeError:
            reader = pd.read_csv(f, dtype=str, chunksize=chunksize,
                                 encoding="latin-1", low_memory=False, na_filter=False)
            total_rows += processChunks(reader,
                                        f.name, output_file, header_written)

    print(
        f"Completado: {total_rows} filas escritas en {output_file} de {len(files)}")
    return 0
