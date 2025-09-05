from pathlib import Path
from typing import Iterable, Optional, Tuple, List
import pandas as pd
import sys
from datetime import datetime
import io
import shutil
import traceback
import hashlib
import pytz

# Constantes de configuración (nombres de carpetas no cambian)
DEFAULT_INPUT = "SubirFormLeadsLinkedin"
DEFAULT_OUTPUT = "FormLeadsLinkedin"
DEFAULT_BACKUP = "FormLeadsLinkedinBK"
GLOB_PATTERN = "*.csv"
CHUNKSIZE = 200_000

ZonaHorariaLocal = pytz.timezone("America/Lima")

# Columnas esperadas
ColumnasEsperadas = [
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
ColumnasSalida = ColumnasEsperadas + ["archivoOrigen"]
ColumnasOrdenCanonico = ColumnasSalida


def ObtenerFechaHoraLocal():
    # Devuelve la hora actual en Lima
    return datetime.now(ZonaHorariaLocal)


def ObtenerRutaLog() -> Path:
    proyecto = Path(__file__).parent.resolve()
    return proyecto / "Logs.txt"


def RegistrarLog(mensaje: str, nivel: str = "INFO"):
    ts = ObtenerFechaHoraLocal().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"{ts} [{nivel}] {mensaje}"
    if nivel.upper() == "ERROR":
        print(linea, file=sys.stderr)
    else:
        print(linea)
    try:
        ruta_log = ObtenerRutaLog()
        ruta_log.parent.mkdir(parents=True, exist_ok=True)
        with ruta_log.open("a", encoding="utf-8") as f:
            f.write(linea + "\n")
    except Exception:
        print(
            f"{ts} [ERROR] No se pudo escribir en Logs.txt: {traceback.format_exc()}",
            file=sys.stderr,
        )


def EncontrarDirectorioLinkedin() -> Optional[Path]:
    punto_inicial = Path(__file__).parent.resolve()
    for archivo in [punto_inicial] + list(punto_inicial.parents):
        if archivo.name == "UnifacionCSVLinkedin":
            posible = (archivo / "Linkedin").resolve()
            return posible
    return None


def GenerarNombreSalida(base_output: str, prefijo: str = "ArchivosLinkedin") -> Path:
    ahora = ObtenerFechaHoraLocal()
    ts = ahora.strftime("%Y%m%d_%H%M%S")
    base = Path(base_output)
    carpeta_salida = base.parent if base.suffix.lower() in (".csv", ".xlsx") else base
    carpeta_salida.mkdir(parents=True, exist_ok=True)
    return carpeta_salida / f"{prefijo}_{ts}.xlsx"


def EsUtf8(b: bytes) -> bool:
    try:
        b.decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False


def CanonicalizarDataFrame(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2 = df2.reindex(columns=ColumnasSalida)
    df2 = df2.fillna("")
    df2 = df2.astype(str)
    for c in df2.select_dtypes(include=["object"]).columns:
        df2[c] = df2[c].str.strip()
    df2 = df2.sort_values(by=ColumnasOrdenCanonico,
                          kind="mergesort").reset_index(drop=True)
    return df2


def CalcularHashDataFrame(df: pd.DataFrame) -> str:
    csv_bytes = CanonicalizarDataFrame(df).to_csv(
        index=False, lineterminator="\n"
    ).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()


def CalcularHashArchivo(path: Path, algo: str = "sha256", chunk_size: int = 8192) -> str:
    h = hashlib.new(algo)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def AbrirTextoBytes(path: Path) -> Tuple[str, str]:
    b = path.read_bytes()
    if EsUtf8(b):
        return b.decode("utf-8"), "utf-8"
    else:
        return b.decode("latin-1"), "latin-1"


def LeerCsvPorChunks(texto: str, chunksize: int, sep: str = ","):
    buf = io.StringIO(texto)
    return pd.read_csv(
        buf, dtype=str, chunksize=chunksize, sep=sep, low_memory=False, na_filter=False
    )


def NormalizarChunk(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk = chunk.astype(str, copy=False)
    for c in chunk.select_dtypes(include=["object"]).columns:
        chunk[c] = chunk[c].str.strip()
    chunk = chunk.reindex(columns=ColumnasEsperadas)
    chunk = chunk.fillna("")
    return chunk


def ObtenerUltimoArchivo(carpeta: Path, patron: str = "*") -> Optional[Path]:
    if not carpeta.exists():
        return None
    archivos = [p for p in carpeta.glob(patron) if p.is_file()]
    if not archivos:
        return None
    archivos.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return archivos[0]


def CopiarABackup(src_path: Path, carpeta_backup: Path) -> Optional[Path]:
    try:
        carpeta_backup.mkdir(parents=True, exist_ok=True)
        destino = carpeta_backup / src_path.name
        if destino.exists():
            # comparar hashes de archivo para evitar duplicados
            try:
                src_hash = CalcularHashArchivo(src_path)
                dest_hash = CalcularHashArchivo(destino)
                if src_hash == dest_hash:
                    RegistrarLog(
                        f"[backup] Archivo idéntico ya existe en backup: {destino}. No se copia.", "INFO")
                    return destino
            except Exception as e:
                RegistrarLog(
                    f"[backup] Error calculando hash antes de copiar: {e}\n{traceback.format_exc()}", "ERROR")
            # si no idéntico, generamos nombre con timestamp para evitar sobreescritura
            destino = carpeta_backup / \
                f"{src_path.stem}_{ObtenerFechaHoraLocal().strftime('%Y%m%d%H%M%S')}{src_path.suffix}"

        shutil.copy2(src_path, destino)
        RegistrarLog(f"[backup] Copiado {src_path.name} -> {destino}", "INFO")
        return destino
    except Exception as e:
        RegistrarLog(
            f"[backup] Error al copiar a backup: {e}\n{traceback.format_exc()}", "ERROR")
        return None


def EliminarArchivosProcesados(archivos: List[Path]):
    for f in archivos:
        try:
            if f.exists() and f.is_file():
                f.unlink()
                RegistrarLog(
                    f"Eliminado archivo de entrada procesado: {f.name}", "INFO")
        except Exception as e:
            RegistrarLog(f"Error eliminando {f}: {e}", "ERROR")


def MoverEliminarArchivos(out_dir: Path, backup_dir: Optional[Path]):
    if not out_dir.exists():
        return
    for item in list(out_dir.glob("*.xlsx")):
        try:
            if item.is_file():
                if backup_dir:
                    destino = backup_dir / item.name
                    if destino.exists():
                        try:
                            item_hash = CalcularHashArchivo(item)
                            dest_hash = CalcularHashArchivo(destino)
                            if item_hash == dest_hash:
                                # si idénticos ya en backup => eliminar item (no mover)
                                item.unlink()
                                RegistrarLog(
                                    f"[rotate] Archivo {item.name} idéntico ya existe en backup; eliminado en out_dir", "INFO")
                                continue
                        except Exception as e:
                            RegistrarLog(
                                f"[rotate] Error calculando hash antes de mover: {e}\n{traceback.format_exc()}", "ERROR")
                        # renombrar destino para mover
                        destino = backup_dir / \
                            f"{item.stem}_{ObtenerFechaHoraLocal().strftime('%Y%m%d%H%M%S')}{item.suffix}"
                    # mover (o mover con nombre nuevo si destino existía y era distinto)
                    shutil.move(str(item), str(destino))
                    RegistrarLog(
                        f"[rotate] Movido {item.name} -> {destino}", "INFO")
                else:
                    # sin backup borrar
                    item.unlink()
                    RegistrarLog(
                        f"[rotate] Eliminado {item.name} en {out_dir}", "INFO")
        except Exception as e:
            RegistrarLog(
                f"[rotate] Error moviendo/eliminando {item}: {e}\n{traceback.format_exc()}", "ERROR")


def UnificarCsvCarpeta(
    carpeta_entrada: str = DEFAULT_INPUT,
    archivo_salida: str = DEFAULT_OUTPUT,
    patron: str = GLOB_PATTERN,
    chunksize: int = CHUNKSIZE,
    sep: str = ",",
    carpeta_backup: Optional[str] = DEFAULT_BACKUP,
) -> Tuple[str, Optional[str]]:
    proyecto = Path(__file__).parent.resolve()

    # detectar / crear directorio Linkedin si no se encuentra
    directorio_linkedin = EncontrarDirectorioLinkedin()
    if directorio_linkedin:
        RegistrarLog(
            f"Directorio LinkedIn detectado en: {directorio_linkedin}", "INFO")
    else:
        try:
            posible_raiz = proyecto.parents[1]
            candidato = (posible_raiz / "Linkedin").resolve()
            candidato.mkdir(parents=True, exist_ok=True)
            directorio_linkedin = candidato
            RegistrarLog(
                f"No se encontró 'UnifacionCSVLinkedin'. Se creó: {directorio_linkedin}", "INFO")
        except Exception:
            candidato = (proyecto / "Linkedin").resolve()
            candidato.mkdir(parents=True, exist_ok=True)
            directorio_linkedin = candidato
            RegistrarLog(
                f"No se pudo crear en ../UnifacionCSVLinkedin/Linkedin. Se creó: {directorio_linkedin}", "INFO")

    # resolver carpeta entrada
    carpeta_in = Path(carpeta_entrada)
    if not carpeta_in.is_absolute():
        carpeta_in = (directorio_linkedin / carpeta_entrada).resolve(
        ) if directorio_linkedin else (proyecto / carpeta_entrada).resolve()
    if not carpeta_in.exists():
        carpeta_in.mkdir(parents=True, exist_ok=True)
        RegistrarLog(f"Carpeta de entrada creada: {carpeta_in}", "INFO")

    # resolver destino / carpeta salida
    destino = Path(archivo_salida)
    if not destino.is_absolute():
        destino = (directorio_linkedin / archivo_salida).resolve(
        ) if directorio_linkedin else (proyecto / archivo_salida).resolve()
    carpeta_out = destino if destino.suffix == "" else destino.parent
    carpeta_out.mkdir(parents=True, exist_ok=True)

    # resolver backup
    backup_path_obj: Optional[Path] = None
    if carpeta_backup:
        carpeta_backup_path = Path(carpeta_backup)
        if not carpeta_backup_path.is_absolute():
            backup_path_obj = (directorio_linkedin / carpeta_backup_path).resolve(
            ) if directorio_linkedin else (proyecto / carpeta_backup_path).resolve()
        else:
            backup_path_obj = carpeta_backup_path

    RegistrarLog(f"CWD: {Path.cwd()}", "INFO")
    RegistrarLog(f"Carpeta entrada: {carpeta_in}", "INFO")
    RegistrarLog(f"Carpeta salida (out): {carpeta_out}", "INFO")
    if backup_path_obj:
        RegistrarLog(f"Carpeta backup: {backup_path_obj}", "INFO")
    else:
        RegistrarLog(
            "Carpeta backup: None (los previos serán eliminados si existen)", "INFO")

    # listar archivos a procesar
    archivos = sorted(carpeta_in.glob(patron)) if carpeta_in.exists() else []
    if not archivos:
        RegistrarLog(
            f"No se encontraron archivos en {carpeta_in} con patrón {patron}", "ERROR")
        return ("", None)

    # leer y normalizar
    lista_chunks = []
    total_filas = 0
    archivos_procesados: List[Path] = []
    for f in archivos:
        try:
            RegistrarLog(f"Procesando {f.name} ...", "INFO")
            texto, encoding = AbrirTextoBytes(f)
            RegistrarLog(f"  encoding detectado: {encoding}", "INFO")
            reader = LeerCsvPorChunks(texto, chunksize=chunksize, sep=sep)
            for chunk in reader:
                chunk = NormalizarChunk(chunk)
                chunk["archivoOrigen"] = f.name
                chunk = chunk.reindex(columns=ColumnasSalida)
                lista_chunks.append(chunk)
                total_filas += len(chunk)
            archivos_procesados.append(f)
        except pd.errors.EmptyDataError:
            RegistrarLog(f"Archivo vacío: {f}", "ERROR")
        except Exception as e:
            RegistrarLog(
                f"ERROR procesando {f}: {e}\n{traceback.format_exc()}", "ERROR")

    if not lista_chunks:
        RegistrarLog("No se procesaron datos", "INFO")
        return ("", None)

    df_final = pd.concat(lista_chunks, ignore_index=True)

    # si no existe, comparar con la última del backup ----------------
    archivo_prev: Optional[Path] = ObtenerUltimoArchivo(
        carpeta_out, patron="ArchivosLinkedin_*.xlsx")
    if not archivo_prev and backup_path_obj and backup_path_obj.exists():
        archivo_prev = ObtenerUltimoArchivo(
            backup_path_obj, patron="ArchivosLinkedin_*.xlsx")

    if archivo_prev:
        try:
            nuevo_hash = CalcularHashDataFrame(df_final)
            df_prev = pd.read_excel(archivo_prev, dtype=str, engine="openpyxl")
            # Asegurar columnas consistentes
            df_prev = df_prev.reindex(
                columns=ColumnasSalida).fillna("").astype(str)
            hash_prev = CalcularHashDataFrame(df_prev)

            RegistrarLog(f"[compare] SHA nuevo: {nuevo_hash}", "INFO")
            RegistrarLog(
                f"[compare] SHA previo (archivo: {archivo_prev.name}): {hash_prev}", "INFO")

            if nuevo_hash == hash_prev:
                # idénticos: no escribir, limpiar inputs y devolver ruta previa
                RegistrarLog(
                    "Contenido idéntico a la versión previa; no se genera nuevo archivo.", "INFO")
                EliminarArchivosProcesados(archivos_procesados)
                return (str(archivo_prev), None)
            # proceder a reemplazo
            RegistrarLog(
                "Contenido diferente: se procederá a reemplazar la versión viva por la nueva.", "INFO")
        except Exception as e:
            RegistrarLog(
                f"[compare] Error comparando: {e}\n{traceback.format_exc()}", "ERROR")
            # continuar con merge

    try:
        # mover/eliminar archivos previos
        MoverEliminarArchivos(carpeta_out, backup_path_obj)

        # escribir nuevo archivo en carpeta_out
        ruta_salida = GenerarNombreSalida(destino)
        df_final.to_excel(ruta_salida, index=False, engine="openpyxl")
        RegistrarLog(
            f"Completado: {total_filas} filas escritas en {ruta_salida}", "INFO")

        # copiar nuevo al backup
        backup_creado = None
        if backup_path_obj:
            backup_creado = CopiarABackup(ruta_salida, backup_path_obj)

        # eliminar archivos CSV procesados
        EliminarArchivosProcesados(archivos_procesados)

        return (str(ruta_salida), str(backup_creado) if backup_creado else None)
    except Exception as e:
        RegistrarLog(
            f"ERROR al escribir {locals().get('ruta_salida', '??')}: {e}\n{traceback.format_exc()}", "ERROR")
        return ("", None)


if __name__ == "__main__":
    salida, backup = UnificarCsvCarpeta()
    RegistrarLog(f"RESULTADO: {salida} BK: {backup}", "INFO")
