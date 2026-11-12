"""Puntuación con los modelos activos y nivel de alerta (verde / ámbar / rojo) -> gold/predicciones.

Uso: python -m latam_ops.ml.predecir [--splits valid,test,demo] [--desde AAAA-MM --hasta AAAA-MM]
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from latam_ops.comun.config import lake
from latam_ops.comun.contratos import aplicar_contrato
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.ml.datasets import cargar_features
from latam_ops.ml.evaluar import con_probabilidad
from latam_ops.ml.registro import cargar_activo
from latam_ops.transformacion.comunes import escribir_particionado, filtro_meses


def ejecutar(splits: list[str], desde: str | None = None, hasta: str | None = None) -> int:
    lk = lake()
    spark = crear_sesion("predecir")
    with registrar_corrida("predecir") as info:
        df = cargar_features(spark, lk)
        df = df.where(F.col("split").isin(splits)) if not desde else df.where(filtro_meses(desde, hasta))
        m15, meta15 = cargar_activo(lk, "final_y_demora15")
        m60, meta60 = cargar_activo(lk, "final_y_demora60")
        u15 = float(meta15.get("umbrales", {}).get("ratio5", 0.5))
        u60 = float(meta60.get("umbrales", {}).get("ratio5", 0.5))
        p15 = con_probabilidad(m15.transform(df), salida="prob_demora15").select("id_vuelo", "prob_demora15")
        p60 = con_probabilidad(m60.transform(df), salida="prob_demora60").select("id_vuelo", "prob_demora60")
        pred = (
            df.select("id_vuelo", "split", "year", "month").join(p15, "id_vuelo").join(p60, "id_vuelo")
            .withColumn("modelo", F.lit("final"))
            .withColumn("version", F.lit(f"{meta15.get('version')}|{meta60.get('version')}"))
            .withColumn("umbral_ambar", F.lit(u15)).withColumn("umbral_rojo", F.lit(u60))
            .withColumn("nivel_alerta", F.when(F.col("prob_demora60") >= u60, "rojo")
                        .when(F.col("prob_demora15") >= u15, "ambar").otherwise("verde"))
        )
        pred = aplicar_contrato(pred, "gold", "predicciones")
        escribir_particionado(pred, lk.ruta("gold_predicciones"), ["year", "month"])
        info["filas_salida"] = spark.read.parquet(lk.ruta("gold_predicciones")).count()
        return info["filas_salida"]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Predicciones y nivel de alerta")
    p.add_argument("--splits", default="valid,test,demo")
    p.add_argument("--desde")
    p.add_argument("--hasta")
    a = p.parse_args(argv)
    ejecutar(a.splits.split(","), a.desde, a.hasta)


if __name__ == "__main__":
    main()
