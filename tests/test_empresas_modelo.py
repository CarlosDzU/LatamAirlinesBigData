"""La carga del grupo LATAM queda fuera del modelo y del valor (config/aerolineas_latam.yaml)."""

from pyspark.sql import functions as F

from latam_ops.comun.config import empresas_latam, empresas_modelo
from latam_ops.transformacion.gold_features import construir_features


def test_empresas_modelo_solo_pasajeros():
    modelo = empresas_modelo()
    assert "TAM" in modelo and "LAN" in modelo
    assert "LTG" not in modelo and "LCO" not in modelo
    assert set(modelo) <= set(empresas_latam())


def test_features_sin_carga(silver_vuelos, clima_horario, cfg):
    # El archivo real de enero 2024 trae vuelos de LATAM Cargo (LTG/LCO).
    assert silver_vuelos.where(F.col("empresa_icao").isin("LTG", "LCO") & F.col("es_latam")).count() > 0
    f = construir_features(silver_vuelos, clima_horario, cfg)
    assert f.where(F.col("empresa_icao").isin("LTG", "LCO")).count() == 0
    assert f.where(F.col("empresa_icao") == "TAM").count() > 0
