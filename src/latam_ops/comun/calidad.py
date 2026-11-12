"""Reglas de calidad (Q1..Qn): resultado por regla, reporte JSON y corte si falla una regla crítica."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from latam_ops.comun.almacen import Almacen
from latam_ops.comun.config import Lake, cargar_yaml, ruta_repo
from latam_ops.comun.log import emitir_metrica, obtener_logger, run_id

_log = obtener_logger("calidad")


class ErrorCalidad(RuntimeError):
    """Una regla crítica no se cumplió: el job termina con código distinto de cero."""


@dataclass
class Regla:
    """Resultado de una regla de calidad."""

    codigo: str
    descripcion: str
    valor: float
    umbral: str
    cumple: bool
    critica: bool = False
    detalle: str = ""


def pct(parte: float, total: float) -> float:
    """Porcentaje seguro (0 si total = 0)."""
    return round(100.0 * parte / total, 4) if total else 0.0


def publicar_reporte(lake: Lake, tabla: str, reglas: list[Regla], extra: dict | None = None) -> dict:
    """Escribe el reporte en silver/_calidad/run_id=<id>/<tabla>.json y en docs/evidencias/calidad/.

    Lanza ErrorCalidad si alguna regla crítica no se cumple.
    """
    reporte = {
        "run_id": run_id(),
        "tabla": tabla,
        "reglas": [asdict(r) for r in reglas],
        "cumple_todo": all(r.cumple for r in reglas),
        **(extra or {}),
    }
    pct_calidad = (extra or {}).get("pct_filas_ok")
    if pct_calidad is None:
        pct_calidad = pct(sum(r.cumple for r in reglas), len(reglas))
    reporte["pct_calidad"] = pct_calidad
    emitir_metrica("pct_calidad", float(pct_calidad), "Percent", {"paso": tabla})
    rel = f"{lake.relativa('silver_calidad')}/run_id={run_id()}/{tabla}.json"
    Almacen(lake).escribir_json(rel, reporte)
    local = ruta_repo(cargar_yaml("pipeline")["evidencias"]["dir_local"], "calidad")
    local.mkdir(parents=True, exist_ok=True)
    (local / f"{tabla}.json").write_text(json.dumps(reporte, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    for r in reglas:
        nivel = _log.info if r.cumple else (_log.error if r.critica else _log.warning)
        nivel(f"{r.codigo} {r.descripcion}: {r.valor} (umbral {r.umbral}) -> {'OK' if r.cumple else 'NO CUMPLE'}")
    fallidas = [r.codigo for r in reglas if r.critica and not r.cumple]
    if fallidas:
        raise ErrorCalidad(f"Reglas críticas incumplidas en {tabla}: {fallidas}")
    return reporte
