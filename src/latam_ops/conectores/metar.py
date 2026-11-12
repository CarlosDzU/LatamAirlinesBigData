"""Conector METAR crudo del archivo IEM (Iowa Environmental Mesonet): texto no estructurado por estación y año.

Uso: python -m latam_ops.conectores.metar --desde 2023-01 --hasta 2025-12 [--estaciones SBGR,SBSP] [--origen local|http]
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from latam_ops.comun.config import cargar_yaml, lake, ruta_repo
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger
from latam_ops.conectores.base import ConectorBase

_log = obtener_logger("conector_metar")


def parametros_iem(estacion: str, anio: int) -> dict:
    """Parámetros de la API asos.py para un año completo en UTC (reportes rutinarios y especiales)."""
    return {
        "station": estacion, "data": "metar", "year1": anio, "month1": 1, "day1": 1,
        "year2": anio + 1, "month2": 1, "day2": 1, "tz": "Etc/UTC", "format": "onlycomma",
        "latlon": "no", "elev": "no", "missing": "empty", "trace": "empty", "direct": "no",
        "report_type": [3, 4],
    }


def validar_metar(ruta: Path) -> None:
    """Comprueba el encabezado station,valid,metar y que haya al menos una observación."""
    with open(ruta, encoding="utf-8") as f:
        encabezado = f.readline().strip()
        primera = f.readline().strip()
    if encabezado != "station,valid,metar":
        raise ValueError(f"{ruta}: encabezado METAR inesperado {encabezado!r}")
    if not primera:
        raise ValueError(f"{ruta}: sin observaciones")


class ConectorMetar(ConectorBase):
    """Ingesta a bronze/metar_iem/year=AAAA/station=ICAO/metar.csv."""

    fuente = "metar_iem"

    def ingerir(self, estacion: str, anio: int):
        rel_raw = f"metar_iem/year={anio}/station={estacion}/metar.csv"
        destino = f"{self.lake.relativa('bronze_metar')}/year={anio}/station={estacion}/metar.csv"
        url = cargar_yaml("fuentes")["metar_iem"]["url"]
        archivo = self.obtener(url, rel_raw, params=parametros_iem(estacion, anio))
        if archivo is None:
            _log.warning(f"sin METAR para {estacion} {anio}")
            return None
        try:
            return self.subir(archivo, destino, f"{url}?station={estacion}&year1={anio}", validar_metar)
        except ValueError as exc:
            _log.warning(str(exc))
            return None


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Ingesta METAR (IEM) a bronze")
    p.add_argument("--desde", required=True)
    p.add_argument("--hasta", required=True)
    p.add_argument("--estaciones", default="", help="lista separada por comas; por defecto estaciones_base")
    p.add_argument("--origen", default="http", choices=["http", "local"])
    p.add_argument("--dir-raw", default=None, help="carpeta local con la misma estructura que bronze (origen local)")
    a = p.parse_args(argv)
    cfg = cargar_yaml("fuentes")["metar_iem"]
    estaciones = [e.strip().upper() for e in a.estaciones.split(",") if e.strip()]
    if not estaciones and a.origen == "local":
        base = Path(a.dir_raw) if a.dir_raw else ruta_repo("data", "raw")
        estaciones = sorted({p.parent.name.split("=")[1] for p in (base / "metar_iem").glob("year=*/station=*/metar.csv")})
    estaciones = estaciones or cfg["estaciones_base"]
    anios = range(int(a.desde[:4]), int(a.hasta[:4]) + 1)
    conector = ConectorMetar(lake(), origen=a.origen, dir_raw=a.dir_raw)
    with registrar_corrida("ingesta_metar") as info:
        resultados = []
        for anio in anios:
            for est in estaciones:
                resultados.append(conector.ingerir(est, anio))
                if a.origen == "http":
                    time.sleep(cfg["pausa_seg"])
        validos = [r for r in resultados if r]
        info["filas_salida"] = sum(r.filas for r in validos)
        info["detalle"] = f"{len(validos)}/{len(resultados)} estación-año"


if __name__ == "__main__":
    main()
