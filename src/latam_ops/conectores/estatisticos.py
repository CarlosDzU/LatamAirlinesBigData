"""Conector ANAC Dados Estatísticos do Transporte Aéreo: pasajeros, asientos y despegues por empresa, ruta y mes.

Se usa para estimar cuántos pasajeros viajan en cada vuelo (VRA no trae pasajeros). La ANAC publica un único
CSV con toda la serie desde 2000 y lo reemplaza en cada actualización, por eso se guarda con la fecha de ingesta.

Uso: python -m latam_ops.conectores.estatisticos [--origen local|http]
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from latam_ops.comun.config import cargar_yaml, lake
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.conectores.base import ConectorBase

ARCHIVO = "Dados_Estatisticos.csv"
COLUMNAS_CLAVE = ("EMPRESA_SIGLA", "ANO", "MES", "AEROPORTO_DE_ORIGEM_SIGLA", "AEROPORTO_DE_DESTINO_SIGLA",
                  "PASSAGEIROS_PAGOS", "DECOLAGENS", "ASSENTOS")


def validar_estatisticos(ruta: Path) -> None:
    """Primera línea 'Atualizado em' y encabezado con las columnas que se usan."""
    with open(ruta, encoding="utf-8-sig") as f:
        primera = f.readline().strip()
        encabezado = f.readline().strip()
    if not primera.startswith("Atualizado"):
        raise ValueError(f"{ruta.name}: primera línea inesperada: {primera[:60]!r}")
    faltan = [c for c in COLUMNAS_CLAVE if f'"{c}"' not in encabezado]
    if faltan:
        raise ValueError(f"{ruta.name}: faltan columnas {faltan}")


class ConectorEstatisticos(ConectorBase):
    """Ingesta a bronze/anac_estatisticos/ingest_date=AAAA-MM-DD/Dados_Estatisticos.csv."""

    fuente = "anac_estatisticos"

    def ingerir(self):
        url = f"{cargar_yaml('fuentes')['anac_estatisticos']['url_base']}/{ARCHIVO}"
        archivo = self.obtener(url, f"anac_estatisticos/{ARCHIVO}")
        if archivo is None:
            raise RuntimeError(f"no disponible: {url}")
        hoy = datetime.now(UTC).date().isoformat()
        destino = f"{self.lake.relativa('bronze_estatisticos')}/ingest_date={hoy}/{ARCHIVO}"
        return self.subir(archivo, destino, url, validar_estatisticos)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Ingesta ANAC Dados Estatísticos a bronze")
    p.add_argument("--origen", default="http", choices=["http", "local"])
    p.add_argument("--dir-raw", default=None)
    a = p.parse_args(argv)
    with registrar_corrida("ingesta_estatisticos") as info:
        r = ConectorEstatisticos(lake(), origen=a.origen, dir_raw=a.dir_raw).ingerir()
        info["filas_salida"] = r.filas
        info["detalle"] = f"{r.destino} ({r.estado})"


if __name__ == "__main__":
    main()
