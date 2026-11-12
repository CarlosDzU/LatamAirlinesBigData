"""Registro de modelos: versión, artefacto Spark ML, metadatos (model card) y puntero al modelo activo."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime

from pyspark.ml import PipelineModel

from latam_ops.comun.almacen import Almacen
from latam_ops.comun.config import Lake, cargar_yaml, ruta_repo
from latam_ops.comun.log import obtener_logger, run_id

_log = obtener_logger("registro")


def nueva_version() -> str:
    """Versión = fecha UTC + commit corto (si hay git)."""
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    try:
        commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, timeout=5,
                                cwd=ruta_repo()).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        commit = ""
    return f"{ts}-{commit}" if commit else ts


def registrar(lk: Lake, nombre: str, modelo: PipelineModel, metadatos: dict, activar: bool = True) -> str:
    """Guarda el modelo en gold/modelos/<nombre>/<version>/ y sus métricas en gold/metricas/ y docs/evidencias/."""
    version = nueva_version()
    base_rel = f"{lk.relativa('gold_modelos')}/{nombre}/{version}"
    modelo.write().overwrite().save(f"{lk.base}/{base_rel}/modelo")
    meta = {"nombre": nombre, "version": version, "run_id": run_id(), "registrado_utc": datetime.now(UTC).isoformat(),
            **metadatos}
    alm = Almacen(lk)
    alm.escribir_json(f"{base_rel}/metadatos.json", meta)
    alm.escribir_json(f"{lk.relativa('gold_metricas')}/{nombre}_{version}.json", meta)
    if activar:
        alm.escribir_json(f"{lk.relativa('gold_modelos')}/{nombre}/activo.json", {"version": version, "umbrales": metadatos.get("umbrales")})
    local = ruta_repo(cargar_yaml("pipeline")["evidencias"]["dir_local"], "metricas")
    local.mkdir(parents=True, exist_ok=True)
    (local / f"{nombre}.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    _log.info(f"modelo registrado {nombre}/{version}")
    return version


def cargar_activo(lk: Lake, nombre: str) -> tuple[PipelineModel, dict]:
    """Carga el modelo activo y sus metadatos."""
    alm = Almacen(lk)
    puntero = alm.leer_json(f"{lk.relativa('gold_modelos')}/{nombre}/activo.json")
    if not puntero:
        raise FileNotFoundError(f"No hay modelo activo para {nombre}")
    base_rel = f"{lk.relativa('gold_modelos')}/{nombre}/{puntero['version']}"
    meta = alm.leer_json(f"{base_rel}/metadatos.json") or {}
    return PipelineModel.load(f"{lk.base}/{base_rel}/modelo"), meta
