"""Fixtures de prueba: SparkSession local y datos REALES copiados de ANAC VRA e IEM (tests/fixtures)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
os.environ.setdefault("LATAM_MODO", "local")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    os.environ["LATAM_LAKE_LOCAL"] = str(tmp_path_factory.mktemp("lake"))
    os.environ["LATAM_LOG_NIVEL"] = "WARNING"
    from latam_ops.comun.sesion_spark import crear_sesion

    os.environ["LATAM_SPARK_MASTER"] = "local[2]"
    s = crear_sesion("tests")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def cfg():
    from latam_ops.comun.config import cargar_yaml

    return cargar_yaml("pipeline")


@pytest.fixture(scope="session")
def bronze_vra(spark):
    from latam_ops.transformacion.silver_vuelos import leer_bronze_vra

    return leer_bronze_vra(spark, [str(FIXTURES / "vra_real_2024-01.csv")])


@pytest.fixture(scope="session")
def silver_vuelos(bronze_vra, cfg):
    from latam_ops.comun.config import empresas_latam
    from latam_ops.comun.contratos import aplicar_contrato
    from latam_ops.transformacion.silver_vuelos import transformar_vuelos

    df = transformar_vuelos(bronze_vra, empresas_latam(), cfg["zona_horaria_vra"], cfg["calidad"]["demora_min"],
                            cfg["calidad"]["demora_max"])
    return aplicar_contrato(df, "silver", "vuelos").cache()


@pytest.fixture(scope="session")
def metar_obs(spark):
    from latam_ops.transformacion.silver_metar import leer_bronze_metar, parsear

    return parsear(leer_bronze_metar(spark, [str(FIXTURES / "metar_real_SBGR_SBSP_2024-01.csv")])).cache()


@pytest.fixture(scope="session")
def clima_horario(metar_obs, cfg):
    from latam_ops.transformacion.silver_metar import construir_clima_horario

    return construir_clima_horario(metar_obs, cfg["clima"]["ffill_max_horas"]).cache()
