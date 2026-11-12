"""silver -> gold/features_salida: una fila por salida LATAM con información disponible a T-2 h.

Regla anti-fuga: toda feature del vuelo i se calcula con datos cuyo instante es < trunc_hora(T_i - 2 h) o,
en el caso del itinerario (congestión programada), con datos conocidos de antemano.
Nunca se usa el horario real, la demora ni la situación del propio vuelo.

Uso: python -m latam_ops.transformacion.gold_features --desde 2023-01 --hasta 2025-12
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from latam_ops.comun.calidad import Regla, pct, publicar_reporte
from latam_ops.comun.config import cargar_yaml, lake
from latam_ops.comun.contratos import aplicar_contrato
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.transformacion.comunes import (
    argumentos,
    escribir_particionado,
    filtro_meses,
    leer_parquet_si_existe,
    mes_anterior,
)

_log = obtener_logger("gold_features")

# Columnas que describen el resultado del propio vuelo: jamás pueden ser features.
COLUMNAS_PROHIBIDAS = {
    "partida_real_utc", "chegada_real_utc", "demora_salida_min", "demora_llegada_min", "situacao_voo",
    "cancelado", "es_atipico", "y_demora15", "y_demora60", "y_clase", "codigo_justificativa",
    "partida_real", "chegada_real", "partida_real_local", "chegada_real_local",
}


def etiqueta_split(col_anio_mes: F.Column, split: dict, demo: str) -> F.Column:
    """train / valid / test / demo / fuera según el mes (AAAA-MM) de la salida programada."""
    return (
        F.when(col_anio_mes == demo, "demo")
        .when(col_anio_mes <= split["train_fin"], "train")
        .when(col_anio_mes <= split["valid_fin"], "valid")
        .when(col_anio_mes <= split["test_fin"], "test")
        .otherwise("fuera")
    )


def _estado_red(vuelos: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Agregados horarios por aeródromo de lo ya ocurrido (salidas reales y llegadas reales), todas las empresas."""
    salidas = (
        vuelos.where(F.col("partida_real_utc").isNotNull() & ~F.col("cancelado"))
        .withColumn("h", F.date_trunc("hour", "partida_real_utc"))
        .groupBy(F.col("origen_icao").alias("aero"), "h")
        .agg(F.count("*").alias("n"), F.sum("demora_salida_min").alias("suma_demora"),
             F.sum((F.col("demora_salida_min") >= 15).cast("long")).alias("n_atrasadas"))
    )
    llegadas = (
        vuelos.where(F.col("chegada_real_utc").isNotNull() & ~F.col("cancelado"))
        .withColumn("h", F.date_trunc("hour", "chegada_real_utc"))
        .groupBy(F.col("destino_icao").alias("aero"), "h")
        .agg(F.sum((F.col("demora_llegada_min") >= 15).cast("long")).alias("n_lleg_atrasadas"))
    )
    return salidas, llegadas


def _unir_ventana(base: DataFrame, agregado: DataFrame, columnas: list[str], horas: int) -> DataFrame:
    """Suma, para cada vuelo, los agregados de las horas [kr - horas, kr - 1] del aeródromo de origen."""
    pares = base.select("id_vuelo", "origen_icao", "kr").withColumn(
        "h", F.explode(F.sequence(F.col("kr") - F.expr(f"INTERVAL {horas} HOURS"), F.col("kr") - F.expr("INTERVAL 1 HOUR"),
                                  F.expr("INTERVAL 1 HOUR"))))
    unido = pares.join(agregado, (pares.origen_icao == agregado.aero) & (pares.h == agregado.h), "left")
    return unido.groupBy("id_vuelo").agg(*[F.sum(c).alias(c) for c in columnas])


def construir_features(vuelos: DataFrame, clima: DataFrame | None, cfg: dict, tasa_hist: DataFrame | None = None) -> DataFrame:
    """Construye features_salida (función pura; los vuelos deben incluir el contexto de días previos).

    vuelos: silver/vuelos (todas las empresas). clima: silver/clima_horario (o None).
    tasa_hist: (origen_icao, hora_local, tasa_hist_origen_hora) calculada solo con train.
    """
    horizonte = int(cfg["horizonte_horas"])
    ventana = int(cfg["ventana_red_horas"])
    latam = vuelos.where("es_latam")
    base = (
        latam.withColumn("t_ref_utc", F.col("partida_prevista_utc") - F.expr(f"INTERVAL {horizonte} HOURS"))
        .withColumn("kr", F.date_trunc("hour", "t_ref_utc"))
        .withColumn("hora_local", F.hour("partida_prevista_local"))
        .withColumn("dia_semana", F.dayofweek("partida_prevista_local"))
        .withColumn("mes", F.month("partida_prevista_local"))
        .withColumn("es_fin_de_semana", F.col("dia_semana").isin(1, 7).cast("int"))
    )
    # Demora del mismo número de vuelo en su ocurrencia anterior, solo si ya había despegado antes de kr.
    w_prev = Window.partitionBy("empresa_icao", "numero_voo", "origen_icao").orderBy("partida_prevista_utc")
    base = (
        base.withColumn("_prev_t", F.lag("partida_prevista_utc").over(w_prev))
        .withColumn("_prev_real", F.lag("partida_real_utc").over(w_prev))
        .withColumn("_prev_dem", F.lag("demora_salida_min").over(w_prev))
    )
    horas_desde_prev = (F.unix_timestamp("partida_prevista_utc") - F.unix_timestamp("_prev_t")) / 3600.0
    conocido = F.col("_prev_real").isNotNull() & (F.col("_prev_real") < F.col("kr")) & horas_desde_prev.between(12, 36)
    base = (
        base.withColumn("prev_dia_conocido", conocido.cast("int"))
        .withColumn("prev_dia_demora", F.when(conocido, F.col("_prev_dem")))
        .drop("_prev_t", "_prev_real", "_prev_dem")
    )
    # Congestión programada (itinerario conocido de antemano), todas las empresas.
    prog = vuelos.select("origen_icao", F.date_trunc("hour", "partida_prevista_local").alias("hl"),
                         F.to_date("partida_prevista_local").alias("dl"))
    cong_h = prog.groupBy("origen_icao", "hl").agg(F.count("*").alias("congestion_hora_origen"))
    cong_d = prog.groupBy("origen_icao", "dl").agg(F.count("*").alias("congestion_dia_origen"))
    base = (
        base.withColumn("hl", F.date_trunc("hour", "partida_prevista_local"))
        .withColumn("dl", F.to_date("partida_prevista_local"))
        .join(cong_h, ["origen_icao", "hl"], "left")
        .join(cong_d, ["origen_icao", "dl"], "left")
        .drop("hl", "dl")
    )
    # Estado observable de la red en el origen durante [kr - ventana, kr).
    salidas, llegadas = _estado_red(vuelos)
    red = _unir_ventana(base, salidas, ["n", "suma_demora", "n_atrasadas"], ventana)
    lleg = _unir_ventana(base, llegadas, ["n_lleg_atrasadas"], ventana)
    base = (
        base.join(red, "id_vuelo", "left").join(lleg, "id_vuelo", "left")
        .withColumn("red_n_salidas", F.coalesce(F.col("n"), F.lit(0)).cast("long"))
        .withColumn("red_demora_media", F.when(F.col("n") > 0, F.col("suma_demora") / F.col("n")))
        .withColumn("red_frac_atrasadas", F.when(F.col("n") > 0, F.col("n_atrasadas") / F.col("n")))
        .withColumn("red_n_llegadas_atrasadas", F.coalesce(F.col("n_lleg_atrasadas"), F.lit(0)).cast("long"))
        .drop("n", "suma_demora", "n_atrasadas", "n_lleg_atrasadas")
    )
    # Clima as-of: la hora k = kr contiene la última observación estrictamente anterior a kr (<= T-2 h).
    campos_o = {"viento_kt": "o_viento_kt", "rafaga_kt": "o_rafaga_kt", "vis_m": "o_vis_m", "techo_ft": "o_techo_ft",
                "temp_c": "o_temp_c", "spread_t_td": "o_spread_t_td", "qnh_hpa": "o_qnh_hpa",
                "presion_tendencia_3h": "o_presion_tendencia_3h", "fen_niebla": "o_fen_niebla",
                "fen_neblina": "o_fen_neblina", "fen_lluvia": "o_fen_lluvia", "fen_tormenta": "o_fen_tormenta",
                "tiene_tempo": "o_tiene_tempo", "horas_desde_obs": "o_horas_desde_obs",
                "categoria_vuelo": "o_categoria_vuelo", "obs_utc": "o_obs_utc"}
    campos_d = {"viento_kt": "d_viento_kt", "vis_m": "d_vis_m", "techo_ft": "d_techo_ft", "fen_niebla": "d_fen_niebla",
                "fen_lluvia": "d_fen_lluvia", "fen_tormenta": "d_fen_tormenta"}
    if clima is not None:
        co = clima.select(F.col("estacion").alias("origen_icao"), F.col("k").alias("kr"),
                          *[F.col(c).alias(a) for c, a in campos_o.items()])
        cd = clima.select(F.col("estacion").alias("destino_icao"), F.col("k").alias("kr"),
                          *[F.col(c).alias(a) for c, a in campos_d.items()])
        base = base.join(co, ["origen_icao", "kr"], "left").join(cd, ["destino_icao", "kr"], "left")
    else:
        for a in list(campos_o.values()) + list(campos_d.values()):
            base = base.withColumn(a, F.lit(None))
    base = base.withColumn("clima_disponible", F.col("o_obs_utc").isNotNull().cast("int"))
    # Codificación histórica (solo train) por origen y hora local.
    if tasa_hist is not None:
        base = base.join(tasa_hist, ["origen_icao", "hora_local"], "left")
    else:
        base = base.withColumn("tasa_hist_origen_hora", F.lit(None).cast("double"))
    anio_mes = F.date_format("partida_prevista_local", "yyyy-MM")
    base = base.withColumn("split", etiqueta_split(anio_mes, cfg["split"], cfg["periodo"]["demo"]))
    # Dataset de modelado: excluye cancelados y atípicos (sin etiqueta válida).
    return base.where(~F.col("cancelado") & ~F.col("es_atipico") & F.col("y_demora15").isNotNull())


def calcular_tasa_hist(vuelos: DataFrame, train_fin: str, suavizado: int = 20) -> DataFrame | None:
    """Tasa de demora >= 15 por (origen, hora local) con suavizado bayesiano, usando solo meses de train."""
    train = vuelos.where("es_latam AND y_demora15 IS NOT NULL AND NOT es_atipico").where(
        F.date_format("partida_prevista_local", "yyyy-MM") <= train_fin)
    if train.limit(1).count() == 0:
        return None
    global_ = train.agg(F.avg("y_demora15")).first()[0]
    return (
        train.groupBy("origen_icao", F.hour("partida_prevista_local").alias("hora_local"))
        .agg(F.sum("y_demora15").alias("s"), F.count("*").alias("n"))
        .withColumn("tasa_hist_origen_hora", (F.col("s") + F.lit(global_ * suavizado)) / (F.col("n") + F.lit(suavizado)))
        .select("origen_icao", "hora_local", "tasa_hist_origen_hora")
    )


def ejecutar(desde: str, hasta: str, spark: SparkSession | None = None) -> dict:
    cfg = cargar_yaml("pipeline")
    lk = lake()
    spark = spark or crear_sesion("gold_features")
    with registrar_corrida("gold_features") as info:
        todos = spark.read.parquet(lk.ruta("silver_vuelos"))
        # Contexto: un mes antes para lags y ventana de red al inicio del intervalo.
        vuelos = todos.where(filtro_meses(mes_anterior(desde), hasta))
        clima = leer_parquet_si_existe(spark, lk.ruta("silver_clima_horario"))
        if clima is not None:
            clima = clima.where(filtro_meses(mes_anterior(desde), hasta))
        tasa = calcular_tasa_hist(todos, cfg["split"]["train_fin"])
        if tasa is None:
            _log.warning("sin meses de train en silver: tasa_hist_origen_hora queda nula")
        feats = construir_features(vuelos, clima, cfg, tasa)
        feats = aplicar_contrato(feats, "gold", "features_salida").where(filtro_meses(desde, hasta)).cache()
        m = feats.agg(F.count("*").alias("n"), F.sum("clima_disponible").alias("con_clima"),
                      F.avg("y_demora15").alias("tasa15")).first()
        n = m["n"] or 0
        reglas = [
            Regla("G1", "filas en gold/features_salida", n, "> 0", n > 0, critica=True),
            Regla("G2", "% vuelos con clima de origen a T-2 h", pct(m["con_clima"] or 0, n), ">= 90",
                  pct(m["con_clima"] or 0, n) >= 100 * cfg["calidad"]["cobertura_clima_min"]),
            Regla("G3", "tasa de demora >= 15 (informativa)", round(m["tasa15"] or 0, 4), "informativa", True),
        ]
        reporte = publicar_reporte(lk, "gold_features", reglas, {"desde": desde, "hasta": hasta})
        escribir_particionado(feats, lk.ruta("gold_features"), ["year", "month"])
        info["filas_salida"], info["detalle"] = n, f"{desde}..{hasta}"
        feats.unpersist()
    return reporte


def main(argv: list[str] | None = None) -> None:
    a = argumentos("silver -> gold/features_salida", argv)
    ejecutar(a.desde, a.hasta)


if __name__ == "__main__":
    main()
