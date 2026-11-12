"""ANAC Dados Estatísticos -> pasajeros por despegue (filas reales de agosto 2011 copiadas del archivo de la ANAC)."""

import csv
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from latam_ops.conectores.estatisticos import validar_estatisticos
from latam_ops.transformacion.silver_pasajeros import leer_bronze, transformar

ARCHIVO = Path(__file__).parent / "fixtures" / "estatisticos_real_2011-08.csv"


def _esperado(destino: str) -> tuple[float, float]:
    """Suma a mano (sin Spark) de pasajeros y despegues regulares de TAM SBGR -> destino."""
    pax = dep = 0.0
    with open(ARCHIVO, encoding="utf-8-sig") as f:
        filas = list(csv.reader(f, delimiter=";"))[2:]
    for r in filas:
        if r[0] == "TAM" and r[5] == "SBGR" and r[11] == destino and r[18] == "REGULAR":
            pax += float(r[19] or 0) + float(r[20] or 0)
            dep += float(r[30] or 0)
    return pax, dep


def test_valida_forma_del_archivo():
    validar_estatisticos(ARCHIVO)


def test_pasajeros_por_despegue(spark):
    bronze = leer_bronze(spark, [str(ARCHIVO)])
    salida = transformar(bronze, ["TAM"], "2011-08", "2011-08")
    assert salida.where(F.col("empresa_icao") != "TAM").count() == 0
    fila = salida.where((F.col("origen_icao") == "SBGR") & (F.col("destino_icao") == "KJFK")).first()
    pax, dep = _esperado("KJFK")
    assert dep > 0
    assert fila["pasajeros"] == pax and fila["despegues"] == dep
    assert fila["pax_por_despegue"] == pytest.approx(pax / dep)
    # fuera del periodo no queda nada
    assert transformar(bronze, ["TAM"], "2012-01", "2012-12").count() == 0
