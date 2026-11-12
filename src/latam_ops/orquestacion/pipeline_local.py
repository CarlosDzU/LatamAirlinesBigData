"""Ejecución completa en la laptop (Spark local[*]) sobre la muestra real de data/muestra o sobre data/raw.

No requiere AWS. Detecta automáticamente los meses VRA disponibles en la carpeta de datos.
Uso: python -m latam_ops.orquestacion.pipeline_local [--dir-raw data/muestra] [--lake data/lake_muestra]
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

from latam_ops.comun.config import ruta_repo
from latam_ops.comun.log import obtener_logger

_log = obtener_logger("pipeline_local")


def meses_disponibles(dir_raw: Path) -> list[str]:
    """Meses AAAA-MM con archivo VRA en <dir_raw>/anac_vra/year=/month=/."""
    meses = set()
    for p in (dir_raw / "anac_vra").rglob("VRA_*.csv"):
        m = re.search(r"year=(\d{4})/month=(\d{2})", p.as_posix())
        if m:
            meses.add(f"{m.group(1)}-{m.group(2)}")
    return sorted(meses)


def preparar_layout(dir_datos: Path, destino: Path) -> Path:
    """Si la carpeta no tiene la estructura de bronze (year=/month=, year=/station=), la arma copiando archivos.

    Acepta la muestra versionada (archivos planos VRA_AAAAM*.csv y metar_<ICAO>_*.csv).
    """
    if any((dir_datos / "anac_vra").glob("year=*/month=*/VRA_*.csv")):
        return dir_datos
    import shutil

    destino.mkdir(parents=True, exist_ok=True)
    for f in (dir_datos / "anac_vra").glob("VRA_*.csv"):
        m = re.match(r"VRA_(\d{4})(\d{1,2})", f.name)
        if m:
            d = destino / "anac_vra" / f"year={m.group(1)}" / f"month={int(m.group(2)):02d}"
            d.mkdir(parents=True, exist_ok=True)
            shutil.copy(f, d / f"VRA_{m.group(1)}{int(m.group(2))}.csv")
    for f in (dir_datos / "metar_iem").glob("metar_*.csv"):
        partes = f.stem.split("_")  # metar_SBGR_2023-12-31_2024-01-08
        estacion, anio = partes[1], partes[-1][:4]
        d = destino / "metar_iem" / f"year={anio}" / f"station={estacion}"
        d.mkdir(parents=True, exist_ok=True)
        shutil.copy(f, d / "metar.csv")
    return destino


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Pipeline completo en local")
    p.add_argument("--dir-raw", default=str(ruta_repo("data", "muestra")))
    p.add_argument("--lake", default=str(ruta_repo("data", "lake_muestra")))
    p.add_argument("--sin-modelo", action="store_true", help="solo ingesta -> silver -> gold")
    a = p.parse_args(argv)
    os.environ["LATAM_MODO"] = "local"
    os.environ["LATAM_LAKE_LOCAL"] = str(Path(a.lake).resolve())
    dir_raw = preparar_layout(Path(a.dir_raw).resolve(), Path(a.lake).resolve() / "_raw_estandar")
    meses = meses_disponibles(dir_raw)
    if not meses:
        raise SystemExit(f"No hay archivos VRA en {dir_raw}/anac_vra/year=AAAA/month=MM/")
    desde, hasta = meses[0], meses[-1]
    _log.info(f"pipeline local {desde}..{hasta} desde {dir_raw} hacia {a.lake}")

    from latam_ops.orquestacion.ejecutar_paso import ejecutar

    for paso in ("ingesta_vra", "ingesta_metar", "silver_vuelos", "silver_metar", "gold_features"):
        ejecutar(paso, desde, hasta, origen="local", dir_raw=str(dir_raw))
    if not a.sin_modelo:
        for paso in ("modelo_base", "modelo_final", "predecir"):
            try:
                ejecutar(paso, desde, hasta, origen="local", dir_raw=str(dir_raw))
            except RuntimeError as exc:
                _log.warning(f"{paso} omitido: {exc}")
    ejecutar("kpi", desde, hasta)
    if not a.sin_modelo:
        try:
            ejecutar("valor", desde, hasta)
        except RuntimeError as exc:
            _log.warning(f"valor omitido: {exc}")
    _log.info("pipeline local terminado")


if __name__ == "__main__":
    main()
