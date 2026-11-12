"""Idempotencia: re-ingestar el mismo archivo no lo duplica; re-escribir particiones no duplica filas."""

from pathlib import Path

from latam_ops.comun.config import lake
from latam_ops.conectores.vra import ConectorVra, url_mes, validar_vra
from latam_ops.transformacion.comunes import escribir_particionado

FIXTURE = Path(__file__).parent / "fixtures" / "vra_real_2024-01.csv"


def test_url_oficial_anac():
    assert url_mes(2023, 1).endswith("/2023/01%20-%20Janeiro/VRA_20231.csv")
    assert "03%20-%20Mar%C3%A7o/VRA_20243.csv" in url_mes(2024, 3)


def test_reingesta_sin_cambios(spark, tmp_path):
    con = ConectorVra(lake(str(tmp_path / "lake")), origen="local")
    destino = "bronze/anac_vra/year=2024/month=01/VRA_20241.csv"
    r1 = con.subir(FIXTURE, destino, "fixture", validar_vra)
    r2 = con.subir(FIXTURE, destino, "fixture", validar_vra)
    assert (r1.estado, r2.estado) == ("nuevo", "sin_cambios") and r1.sha256 == r2.sha256


def test_reescritura_particionada(spark, silver_vuelos, tmp_path):
    ruta = str(tmp_path / "silver_vuelos")
    escribir_particionado(silver_vuelos, ruta, ["year", "month"])
    escribir_particionado(silver_vuelos, ruta, ["year", "month"])
    assert spark.read.parquet(ruta).count() == silver_vuelos.count()
