"""Conector ANAC VRA (Voo Regular Ativo): un CSV mensual con todos los vuelos regulares de Brasil.

Uso: python -m latam_ops.conectores.vra --desde 2023-01 --hasta 2025-12 [--origen local|http] [--landing]
"""

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import quote

from latam_ops.comun.config import cargar_yaml, lake, periodo_meses
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger
from latam_ops.conectores.base import ConectorBase

_log = obtener_logger("conector_vra")
ENCABEZADO = "ICAO Empresa Aérea"


def url_mes(anio: int, mes: int) -> str:
    """URL oficial del CSV de un mes (el nombre del archivo lleva el mes sin cero)."""
    cfg = cargar_yaml("fuentes")["anac_vra"]
    nombre_mes = cfg["meses_pt"][mes - 1]
    return f"{cfg['url_base']}/{anio}/{mes:02d}%20-%20{quote(nombre_mes)}/VRA_{anio}{mes}.csv"


def validar_vra(ruta: Path) -> None:
    """Comprueba la forma del archivo: línea 'Atualizado em' y encabezado VRA con 12 columnas."""
    with open(ruta, encoding="utf-8-sig") as f:
        primera = f.readline().strip()
        segunda = f.readline().strip()
    if not primera.startswith("Atualizado"):
        raise ValueError(f"{ruta.name}: primera línea inesperada: {primera[:60]!r}")
    if ENCABEZADO not in segunda or segunda.count(";") != 11:
        raise ValueError(f"{ruta.name}: encabezado VRA inesperado: {segunda[:120]!r}")


class ConectorVra(ConectorBase):
    """Ingesta de ANAC VRA a bronze/anac_vra/year=AAAA/month=MM/VRA_AAAAM.csv."""

    fuente = "anac_vra"

    def ingerir_mes(self, anio: int, mes: int, landing: bool = False):
        nombre = f"VRA_{anio}{mes}.csv"
        rel_raw = f"anac_vra/year={anio}/month={mes:02d}/{nombre}"
        base = self.lake.relativa("landing") + "/anac_vra" if landing else self.lake.relativa("bronze_vra")
        destino = f"{base}/year={anio}/month={mes:02d}/{nombre}" if not landing else f"{base}/{nombre}"
        url = url_mes(anio, mes)
        archivo = self.obtener(url, rel_raw)
        if archivo is None:
            _log.warning(f"no disponible: {url}")
            return None
        return self.subir(archivo, destino, url, validar_vra)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Ingesta ANAC VRA a bronze")
    p.add_argument("--desde", required=True)
    p.add_argument("--hasta", required=True)
    p.add_argument("--origen", default="http", choices=["http", "local"])
    p.add_argument("--dir-raw", default=None, help="carpeta local con la misma estructura que bronze (origen local)")
    p.add_argument("--landing", action="store_true", help="deja el archivo en la zona landing (demo)")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    meses = periodo_meses(a.desde, a.hasta)
    if a.dry_run:
        for anio, mes in meses:
            print(url_mes(anio, mes))
        return
    conector = ConectorVra(lake(), origen=a.origen, dir_raw=a.dir_raw)
    with registrar_corrida("ingesta_vra") as info:
        resultados = [conector.ingerir_mes(anio, mes, landing=a.landing) for anio, mes in meses]
        validos = [r for r in resultados if r]
        info["filas_salida"] = sum(r.filas for r in validos)
        info["detalle"] = f"{len(validos)}/{len(meses)} meses; nuevos={sum(r.estado == 'nuevo' for r in validos)}"


if __name__ == "__main__":
    main()
