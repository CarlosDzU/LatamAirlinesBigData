"""clima_horario: el valor de la hora k siempre es una observación estrictamente anterior a k (sin fuga)."""

from pyspark.sql import functions as F


def test_observacion_estrictamente_anterior(clima_horario):
    malas = clima_horario.where(F.col("obs_utc").isNotNull() & (F.col("obs_utc") >= F.col("k"))).count()
    assert malas == 0


def test_ffill_maximo(clima_horario, cfg):
    limite = cfg["clima"]["ffill_max_horas"] + 1
    assert clima_horario.where(F.col("horas_desde_obs") > limite).count() == 0


def test_grilla_horaria_completa(clima_horario):
    por_est = clima_horario.groupBy("estacion").agg(F.count("*").alias("n"), F.countDistinct("k").alias("d"))
    assert all(r["n"] == r["d"] for r in por_est.collect())


def test_features_usan_clima_antes_de_t_menos_2h(silver_vuelos, clima_horario, cfg):
    from latam_ops.transformacion.gold_features import construir_features

    f = construir_features(silver_vuelos, clima_horario, cfg)
    con = f.where("o_obs_utc IS NOT NULL") if "o_obs_utc" in f.columns else f.where("clima_disponible = 1")
    assert con.count() > 0
    assert con.where(F.col("o_obs_utc") >= F.col("t_ref_utc")).count() == 0
