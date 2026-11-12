"""Registro de corridas (deja constancia de cada ejecución)."""

from __future__ import annotations

import csv
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

from latam_ops.comun.config import cargar_yaml, modo, ruta_repo
from latam_ops.comun.log import emitir_metrica, obtener_logger, run_id

_CAMPOS = ["ts_inicio", "run_id", "modo", "paso", "estado", "filas_entrada", "filas_salida", "segundos", "detalle"]
_log = obtener_logger("corridas")


@contextmanager
def registrar_corrida(paso: str) -> Iterator[dict]:
    """Context manager: mide el tiempo y agrega una línea a docs/evidencias/corridas.csv.

    El bloque puede completar `info['filas_entrada']`, `info['filas_salida']` y `info['detalle']`.
    """
    info: dict = {"filas_entrada": None, "filas_salida": None, "detalle": ""}
    inicio = time.time()
    ts = datetime.now(UTC).isoformat(timespec="seconds")
    estado = "ok"
    try:
        yield info
    except Exception as exc:
        estado = "error"
        info["detalle"] = f"{type(exc).__name__}: {exc}"[:500]
        raise
    finally:
        segundos = round(time.time() - inicio, 2)
        fila = {"ts_inicio": ts, "run_id": run_id(), "modo": modo(), "paso": paso, "estado": estado,
                "filas_entrada": info.get("filas_entrada"), "filas_salida": info.get("filas_salida"),
                "segundos": segundos, "detalle": info.get("detalle", "")}
        _escribir(fila)
        _log.info(f"paso {paso} {estado} en {segundos}s", extra={"datos": fila})
        emitir_metrica("segundos", segundos, "Seconds", {"paso": paso})
        if info.get("filas_salida") is not None:
            emitir_metrica("filas_out", float(info["filas_salida"]), "Count", {"paso": paso})


def _escribir(fila: dict) -> None:
    directorio = ruta_repo(cargar_yaml("pipeline")["evidencias"]["dir_local"])
    directorio.mkdir(parents=True, exist_ok=True)
    archivo = directorio / "corridas.csv"
    nuevo = not archivo.exists()
    with open(archivo, "a", newline="", encoding="utf-8") as f:
        escritor = csv.DictWriter(f, fieldnames=_CAMPOS)
        if nuevo:
            escritor.writeheader()
        escritor.writerow(fila)
