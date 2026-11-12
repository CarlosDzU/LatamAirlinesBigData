"""Anti-fuga: ninguna feature del modelo describe el resultado del propio vuelo."""

import yaml

from latam_ops.comun.config import ruta_repo
from latam_ops.ml.datasets import columnas_features
from latam_ops.transformacion.gold_features import COLUMNAS_PROHIBIDAS


def test_features_no_intersectan_prohibidas():
    num, cat = columnas_features()
    assert not (set(num) | set(cat)) & COLUMNAS_PROHIBIDAS


def test_etiquetas_no_son_features():
    num, cat = columnas_features()
    contrato = yaml.safe_load(open(ruta_repo("contratos", "gold", "features_salida.yaml"), encoding="utf-8"))
    assert not set(contrato["etiquetas"]) & (set(num) | set(cat))


def test_vector_de_features_sin_columnas_prohibidas(spark):
    from latam_ops.ml.preparacion import etapas

    entradas = set()
    for e in etapas():
        for metodo in ("getInputCols", "getInputCol"):
            if hasattr(e, metodo):
                try:
                    v = getattr(e, metodo)()
                    entradas |= set(v if isinstance(v, list) else [v])
                except Exception:
                    pass
    base = {c.removesuffix("_imp").removesuffix("_idx").removesuffix("_ohe") for c in entradas}
    assert not base & COLUMNAS_PROHIBIDAS


def test_tasa_hist_fuera_de_muestra(silver_vuelos, clima_horario, cfg):
    """La tasa histórica de un vuelo de train no puede usar su propio mes (codificación fuera de muestra)."""
    from pyspark.sql import functions as F

    from latam_ops.transformacion.gold_features import calcular_tasa_hist, construir_features

    # Con train_fin = 2024-01 el único mes de train es el propio: no queda historia y la tasa es el prior.
    tasa = calcular_tasa_hist(silver_vuelos, "2024-01")
    prior = tasa.first()["prior"]
    f = construir_features(silver_vuelos, clima_horario, cfg, tasa)
    ene = f.where(F.date_format("partida_prevista_local", "yyyy-MM") == "2024-01")
    assert ene.count() > 0
    distintos = ene.where(F.abs(F.col("tasa_hist_origen_hora") - F.lit(prior)) > 1e-9).count()
    assert distintos == 0
