"""Horarios VRA (hora de Brasília, UTC-3 fijo) a UTC, demora y etiquetas, con filas reales de ene-2024."""

from pyspark.sql import functions as F


def test_lectura_descarta_encabezados(bronze_vra):
    primeras = [r["empresa_icao"] for r in bronze_vra.select("empresa_icao").limit(5).collect()]
    assert all(len(p) == 3 for p in primeras)


def test_utc_es_local_mas_3h(silver_vuelos):
    dif = silver_vuelos.select(((F.unix_timestamp("partida_prevista_utc") - F.unix_timestamp("partida_prevista_local")) / 3600).alias("h")).distinct().collect()
    assert [r["h"] for r in dif] == [3.0]


def test_demora_y_etiquetas(silver_vuelos):
    filas = silver_vuelos.where("NOT cancelado AND demora_salida_min IS NOT NULL").select(
        "demora_salida_min", "y_demora15", "y_demora60", "y_clase").collect()
    assert filas
    for r in filas:
        assert r["y_demora15"] == int(r["demora_salida_min"] >= 15)
        assert r["y_demora60"] == int(r["demora_salida_min"] >= 60)
        assert r["y_clase"] == r["y_demora15"] + r["y_demora60"]


def test_cancelados_sin_etiqueta(silver_vuelos):
    canc = silver_vuelos.where("cancelado")
    assert canc.count() > 0
    assert canc.where("y_demora15 IS NOT NULL OR demora_salida_min IS NOT NULL").count() == 0


def test_particion_por_mes_local(silver_vuelos):
    assert {(r["year"], r["month"]) for r in silver_vuelos.select("year", "month").distinct().collect()} == {(2024, 1)}
