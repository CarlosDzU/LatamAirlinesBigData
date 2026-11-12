"""silver + predicciones -> modelo estrella en gold: fact_vuelo (grano: una salida programada LATAM) y dimensiones.

Uso: python -m latam_ops.transformacion.gold_dimensional --desde 2023-01 --hasta 2026-01
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from latam_ops.comun.config import empresas_latam, lake
from latam_ops.comun.contratos import aplicar_contrato
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.transformacion.comunes import (
    argumentos,
    escribir_particionado,
    filtro_meses,
    leer_parquet_si_existe,
)

PREFIJOS_BRASIL = ("SB", "SD", "SI", "SJ", "SN", "SS", "SW")
CONDICIONES = [
    ("VFR", "VFR", "Visual: techo > 3000 ft y visibilidad > 5 mi", 3000, 8047),
    ("MVFR", "MVFR", "Visual marginal: techo 1000-3000 ft o visibilidad 3-5 mi", 1000, 4828),
    ("IFR", "IFR", "Instrumental: techo 500-999 ft o visibilidad 1-3 mi", 500, 1609),
    ("LIFR", "LIFR", "Instrumental bajo: techo < 500 ft o visibilidad < 1 mi", 0, 0),
    ("SIN_DATO", "SIN_DATO", "Sin observación METAR disponible a T-2 h", None, None),
]


def construir_fact(vuelos: DataFrame, features: DataFrame | None, pred: DataFrame | None) -> DataFrame:
    """fact_vuelo: todas las salidas LATAM (incluidas canceladas) con su predicción si existe."""
    f = vuelos.where("es_latam")
    if features is not None:
        f = f.join(features.select("id_vuelo", "red_n_llegadas_atrasadas", "o_categoria_vuelo"), "id_vuelo", "left")
    else:
        f = f.withColumn("red_n_llegadas_atrasadas", F.lit(None)).withColumn("o_categoria_vuelo", F.lit(None))
    if pred is not None:
        f = f.join(pred.select("id_vuelo", "prob_demora15", "prob_demora60", "nivel_alerta"), "id_vuelo", "left")
    else:
        for c in ("prob_demora15", "prob_demora60", "nivel_alerta"):
            f = f.withColumn(c, F.lit(None))
    return (
        f.withColumn("fecha_key", F.date_format("partida_prevista_local", "yyyyMMdd").cast("int"))
        .withColumn("origen_key", F.col("origen_icao")).withColumn("destino_key", F.col("destino_icao"))
        .withColumn("aerolinea_key", F.col("empresa_icao"))
        .withColumn("ruta_key", F.concat_ws("-", "origen_icao", "destino_icao"))
        .withColumn("condicion_clima_key", F.coalesce(F.col("o_categoria_vuelo"), F.lit("SIN_DATO")))
        .withColumn("hora_prog_local", F.hour("partida_prevista_local"))
        .withColumn("dep_delay_min", F.col("demora_salida_min"))
        .withColumn("es_demora15", F.col("y_demora15")).withColumn("es_demora60", F.col("y_demora60"))
        .withColumn("cancelado", F.col("cancelado").cast("int"))
        .withColumn("llegada_previa_atrasada", (F.col("red_n_llegadas_atrasadas") > 0).cast("int"))
    )


def construir_dimensiones(spark: SparkSession, vuelos: DataFrame, metar: DataFrame | None) -> dict[str, DataFrame]:
    """Dimensiones conformadas a partir de los vuelos observados."""
    latam = vuelos.where("es_latam")
    fechas = latam.agg(F.min(F.to_date("partida_prevista_local")).alias("a"), F.max(F.to_date("partida_prevista_local")).alias("b"))
    dim_fecha = (
        fechas.select(F.explode(F.sequence("a", "b")).alias("fecha"))
        .withColumn("fecha_key", F.date_format("fecha", "yyyyMMdd").cast("int"))
        .withColumn("anio", F.year("fecha")).withColumn("mes", F.month("fecha")).withColumn("dia", F.dayofmonth("fecha"))
        .withColumn("dia_semana", F.dayofweek("fecha")).withColumn("nombre_dia", F.date_format("fecha", "EEEE"))
        .withColumn("es_fin_de_semana", F.col("dia_semana").isin(1, 7).cast("int"))
        .withColumn("trimestre", F.quarter("fecha"))
    )
    hubs = [r["origen_icao"] for r in latam.groupBy("origen_icao").count().orderBy(F.desc("count")).limit(5).collect()]
    estaciones = [r["estacion"] for r in metar.select("estacion").distinct().collect()] if metar is not None else []
    aeros = latam.select(F.col("origen_icao").alias("icao")).union(latam.select(F.col("destino_icao").alias("icao"))).distinct()
    dim_aeropuerto = (
        aeros.withColumn("origen_key", F.col("icao")).withColumn("pais_prefijo", F.substring("icao", 1, 2))
        .withColumn("es_brasil", F.col("pais_prefijo").isin(*PREFIJOS_BRASIL))
        .withColumn("es_hub", F.col("icao").isin(hubs)).withColumn("tiene_metar", F.col("icao").isin(estaciones))
    )
    latam_cod = empresas_latam()
    dim_aerolinea = (
        vuelos.select("empresa_icao").distinct().withColumn("aerolinea_key", F.col("empresa_icao"))
        .withColumn("es_latam", F.col("empresa_icao").isin(latam_cod))
        .withColumn("grupo", F.when(F.col("es_latam"), "LATAM Airlines Group").otherwise("Otras"))
    )
    dim_ruta = (
        latam.groupBy("origen_icao", "destino_icao").agg(F.count("*").alias("n_vuelos"))
        .withColumn("ruta_key", F.concat_ws("-", "origen_icao", "destino_icao"))
        .withColumn("es_internacional", ~(F.substring("origen_icao", 1, 2).isin(*PREFIJOS_BRASIL)
                                          & F.substring("destino_icao", 1, 2).isin(*PREFIJOS_BRASIL)))
    )
    dim_clima = spark.createDataFrame(CONDICIONES, "condicion_clima_key string, categoria_vuelo string, descripcion string, techo_ft_min int, vis_m_min int")
    return {"dim_fecha": dim_fecha, "dim_aeropuerto": dim_aeropuerto, "dim_aerolinea": dim_aerolinea,
            "dim_ruta": dim_ruta, "dim_condicion_clima": dim_clima}


def ejecutar(desde: str, hasta: str, spark: SparkSession | None = None) -> int:
    lk = lake()
    spark = spark or crear_sesion("gold_dimensional")
    with registrar_corrida("gold_dimensional") as info:
        todos = spark.read.parquet(lk.ruta("silver_vuelos"))
        features = leer_parquet_si_existe(spark, lk.ruta("gold_features"))
        pred = leer_parquet_si_existe(spark, lk.ruta("gold_predicciones"))
        fact = construir_fact(todos.where(filtro_meses(desde, hasta)), features, pred)
        fact = aplicar_contrato(fact, "gold", "fact_vuelo")
        escribir_particionado(fact, lk.ruta("gold_fact_vuelo"), ["year", "month"])
        # Las dimensiones se reconstruyen completas (son pequeñas).
        for nombre, df in construir_dimensiones(spark, todos, leer_parquet_si_existe(spark, lk.ruta("silver_metar"))).items():
            aplicar_contrato(df, "gold", nombre).coalesce(1).write.mode("overwrite").parquet(lk.ruta(f"gold_{nombre}"))
        info["filas_salida"] = spark.read.parquet(lk.ruta("gold_fact_vuelo")).where(filtro_meses(desde, hasta)).count()
        return info["filas_salida"]


def main(argv: list[str] | None = None) -> None:
    a = argumentos("silver -> modelo estrella en gold", argv)
    ejecutar(a.desde, a.hasta)


if __name__ == "__main__":
    main()
