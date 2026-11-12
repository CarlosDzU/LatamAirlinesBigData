"""gold/fact_vuelo -> gold/kpi_diario (K1, K2, K3) y gold/kpi_alerta_mensual (K4).

K1 puntualidad de salida = salidas realizadas con demora < 15 min / salidas realizadas.
K2 alertas emitidas (ámbar y roja) por día y aeródromo.
K3 índice de propagación = % de salidas atrasadas (>= 15) precedidas por llegadas atrasadas al mismo aeródromo
   en las 2 h anteriores a T-2 h (la red ya venía atrasada).
K4 calidad de la alerta roja por mes: precisión y recall (requiere conocer el resultado real).

Uso: python -m latam_ops.transformacion.gold_kpi --desde 2023-01 --hasta 2026-01
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from latam_ops.comun.config import lake
from latam_ops.comun.contratos import aplicar_contrato
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.transformacion.comunes import (
    argumentos,
    escribir_particionado,
    filtro_meses,
    leer_parquet_si_existe,
)


def kpi_diario(fact):
    """Agrega fact_vuelo a (fecha, origen)."""
    realizada = (F.col("cancelado") == 0) & F.col("dep_delay_min").isNotNull()
    return (
        fact.withColumn("fecha", F.to_date(F.col("fecha_key").cast("string"), "yyyyMMdd"))
        .groupBy("fecha", F.col("origen_key").alias("origen_icao"))
        .agg(
            F.count("*").alias("salidas_programadas"),
            F.sum(realizada.cast("long")).alias("salidas_realizadas"),
            F.sum("cancelado").cast("long").alias("cancelaciones"),
            F.sum((realizada & (F.col("dep_delay_min") < 15)).cast("long")).alias("_puntuales"),
            F.avg(F.when(realizada, F.col("dep_delay_min"))).alias("demora_media_min"),
            F.sum((F.col("nivel_alerta") == "ambar").cast("long")).alias("k2_alertas_ambar"),
            F.sum((F.col("nivel_alerta") == "rojo").cast("long")).alias("k2_alertas_rojas"),
            F.sum((F.col("es_demora15") == 1).cast("long")).alias("_atrasadas"),
            F.sum(((F.col("es_demora15") == 1) & (F.col("llegada_previa_atrasada") == 1)).cast("long")).alias("_prop"),
        )
        .withColumn("k1_puntualidad", F.when(F.col("salidas_realizadas") > 0, F.col("_puntuales") / F.col("salidas_realizadas")))
        .withColumn("k3_indice_propagacion", F.when(F.col("_atrasadas") > 0, F.col("_prop") / F.col("_atrasadas")))
        .withColumn("k2_alertas_ambar", F.coalesce("k2_alertas_ambar", F.lit(0)))
        .withColumn("k2_alertas_rojas", F.coalesce("k2_alertas_rojas", F.lit(0)))
        .withColumn("year", F.year("fecha")).withColumn("month", F.month("fecha"))
    )


def kpi_alerta(pred, feats):
    """K4 por mes: alerta roja (prob_demora60 >= umbral_rojo) contra la demora real >= 60."""
    d = pred.join(feats.select("id_vuelo", "y_demora60"), "id_vuelo")
    alerta = (F.col("nivel_alerta") == "rojo").cast("int")
    return (
        d.groupBy(F.format_string("%04d-%02d", "year", "month").alias("anio_mes"), "modelo", "version")
        .agg(F.sum(((alerta == 1) & (F.col("y_demora60") == 1)).cast("long")).alias("vp"),
             F.sum(((alerta == 1) & (F.col("y_demora60") == 0)).cast("long")).alias("fp"),
             F.sum(((alerta == 0) & (F.col("y_demora60") == 1)).cast("long")).alias("fn"),
             F.sum(((alerta == 0) & (F.col("y_demora60") == 0)).cast("long")).alias("vn"))
        .withColumn("k4_precision", F.when(F.col("vp") + F.col("fp") > 0, F.col("vp") / (F.col("vp") + F.col("fp"))))
        .withColumn("k4_recall", F.when(F.col("vp") + F.col("fn") > 0, F.col("vp") / (F.col("vp") + F.col("fn"))))
    )


def ejecutar(desde: str, hasta: str, spark: SparkSession | None = None) -> int:
    lk = lake()
    spark = spark or crear_sesion("gold_kpi")
    with registrar_corrida("gold_kpi") as info:
        fact = spark.read.parquet(lk.ruta("gold_fact_vuelo")).where(filtro_meses(desde, hasta))
        diario = aplicar_contrato(kpi_diario(fact), "gold", "kpi_diario")
        escribir_particionado(diario, lk.ruta("gold_kpi_diario"), ["year", "month"])
        pred = leer_parquet_si_existe(spark, lk.ruta("gold_predicciones"))
        feats = leer_parquet_si_existe(spark, lk.ruta("gold_features"))
        if pred is not None and feats is not None:
            aplicar_contrato(kpi_alerta(pred, feats), "gold", "kpi_alerta_mensual").coalesce(1).write.mode("overwrite").parquet(
                lk.ruta("gold_kpi_alerta_mensual"))
        info["filas_salida"] = diario.count()
        return info["filas_salida"]


def main(argv: list[str] | None = None) -> None:
    a = argumentos("gold/fact_vuelo -> KPI", argv)
    ejecutar(a.desde, a.hasta)


if __name__ == "__main__":
    main()
