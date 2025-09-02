# main.py
import argparse
import sys
from pathlib import Path

from concatCsvs import CHUNKSIZE, DEFAULT_INPUT, DEFAULT_OUTPUT, GLOB_PATTERN, concatCSVFolder


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Concat CSVs de una carpeta en un único CSV (sin formateo)")
    parser.add_argument(
        "--input", "-i", default=DEFAULT_INPUT, help="Carpeta con CSVs")
    parser.add_argument(
        "--output", "-o", default=DEFAULT_OUTPUT, help="Archivo CSV de salida")
    parser.add_argument("--pattern", "-p", default=GLOB_PATTERN,
                        help="Patrón glob (ej: '*.csv')")
    parser.add_argument("--chunksize", "-c", type=int,
                        default=CHUNKSIZE, help="Filas por chunk")

    # Borrar archivo de salida existente
    parser.add_argument("--overwrite", action="store_true",
                        help="Borrar el archivo de salida existente antes de concatenar")

    args = parser.parse_args(argv)

    out_path = Path(args.output)
    if args.overwrite and out_path.exists():
        out_path.unlink()

    return concatCSVFolder(args.input, args.output, args.pattern, args.chunksize)


if __name__ == "__main__":
    sys.exit(main())
