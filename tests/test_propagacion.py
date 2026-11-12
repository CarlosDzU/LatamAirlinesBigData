"""Estado de la red en el origen: solo usa salidas/llegadas reales anteriores a trunc(T-2h)."""

from pyspark.sql import functions as F

from latam_ops.transformacion.gold_features import construir_features


def test_features_red_invariantes_al_futuro(silver_vuelos, clima_horario, cfg):
    base = construir_features(silver_vuelos, clima_horario, cfg).select(
        "id_vuelo", "red_n_salidas", "red_demora_media", "red_n_llegadas_atrasadas", "prev_dia_demora").cache()
    # Se "mueve al futuro" toda salida real posterior a un corte: las features de vuelos con kr <= corte no cambian.
    corte = "2024-01-04 12:00:00"
    alterado = silver_vuelos.withColumn(
        "partida_real_utc", F.when(F.col("partida_real_utc") >= F.lit(corte).cast("timestamp"),
                                   F.col("partida_real_utc") + F.expr("INTERVAL 7 HOURS")).otherwise(F.col("partida_real_utc")))
    nuevo = construir_features(alterado, clima_horario, cfg).where(
        F.date_trunc("hour", F.col("partida_prevista_utc") - F.expr("INTERVAL 2 HOURS")) <= F.lit(corte).cast("timestamp")
    ).select("id_vuelo", "red_n_salidas", "red_demora_media", "red_n_llegadas_atrasadas", "prev_dia_demora")
    comparacion = nuevo.alias("n").join(base.alias("b"), "id_vuelo")
    assert comparacion.count() > 0
    distintos = comparacion.where(
        (F.col("n.red_n_salidas") != F.col("b.red_n_salidas"))
        | ~F.col("n.red_demora_media").eqNullSafe(F.col("b.red_demora_media"))
        | (F.col("n.red_n_llegadas_atrasadas") != F.col("b.red_n_llegadas_atrasadas"))
        | ~F.col("n.prev_dia_demora").eqNullSafe(F.col("b.prev_dia_demora"))
    ).count()
    assert distintos == 0


def test_prev_dia_solo_si_ya_despego(silver_vuelos, clima_horario, cfg):
    f = construir_features(silver_vuelos, clima_horario, cfg)
    assert f.where("prev_dia_conocido = 0 AND prev_dia_demora IS NOT NULL").count() == 0


def test_hay_senal_de_red(silver_vuelos, clima_horario, cfg):
    f = construir_features(silver_vuelos, clima_horario, cfg)
    assert f.where("red_n_salidas > 0").count() > 0
