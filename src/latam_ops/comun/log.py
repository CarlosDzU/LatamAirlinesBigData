"""Logging estructurado (JSON por línea) con run_id, apto para CloudWatch Logs."""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import UTC, datetime

_RUN_ID = os.environ.get("LATAM_RUN_ID") or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def run_id() -> str:
    """Identificador de la corrida actual (compartido por todos los pasos del proceso)."""
    return _RUN_ID


class _FormatoJson(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        datos = {
            "ts": datetime.now(UTC).isoformat(timespec="seconds"),
            "nivel": record.levelname,
            "modulo": record.name,
            "run_id": _RUN_ID,
            "mensaje": record.getMessage(),
        }
        extra = getattr(record, "datos", None)
        if isinstance(extra, dict):
            datos.update(extra)
        return json.dumps(datos, ensure_ascii=False, default=str)


def obtener_logger(nombre: str) -> logging.Logger:
    """Logger con salida JSON a stdout."""
    logger = logging.getLogger(nombre)
    if not logger.handlers:
        manejador = logging.StreamHandler(sys.stdout)
        manejador.setFormatter(_FormatoJson())
        logger.addHandler(manejador)
        dir_logs = os.environ.get("LATAM_LOG_DIR")
        if dir_logs:
            try:
                os.makedirs(dir_logs, exist_ok=True)
                archivo = logging.FileHandler(os.path.join(dir_logs, "latam-ops.log"), encoding="utf-8")
                archivo.setFormatter(_FormatoJson())
                logger.addHandler(archivo)
            except OSError:
                pass
        logger.setLevel(os.environ.get("LATAM_LOG_NIVEL", "INFO"))
        logger.propagate = False
    return logger


def emitir_metrica(nombre: str, valor: float, unidad: str = "Count", dimensiones: dict[str, str] | None = None) -> None:
    """Imprime una métrica en formato CloudWatch EMF (el agente de CloudWatch la convierte en métrica)."""
    dims = dimensiones or {}
    doc = {
        "_aws": {
            "Timestamp": int(datetime.now(UTC).timestamp() * 1000),
            "CloudWatchMetrics": [{"Namespace": "LatamOps", "Dimensions": [list(dims.keys())],
                                   "Metrics": [{"Name": nombre, "Unit": unidad}]}],
        },
        nombre: valor,
        "run_id": _RUN_ID,
        **dims,
    }
    linea = json.dumps(doc)
    print(linea, flush=True)
    dir_logs = os.environ.get("LATAM_LOG_DIR")
    if dir_logs:
        try:
            with open(os.path.join(dir_logs, "metricas_emf.log"), "a", encoding="utf-8") as f:
                f.write(linea + "\n")
        except OSError:
            pass
