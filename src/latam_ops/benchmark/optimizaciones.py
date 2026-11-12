"""Optimizaciones medidas antes/después (sección 3.6):

O1 broadcast join vs sort-merge join (vuelos x aeródromos).
O2 poda de particiones y columnas vs lectura completa.
O3 cache() de features antes de varias agregaciones (como en la búsqueda de hiperparámetros) vs sin cache.
O4 repartition por la clave de la ventana antes de la ventana vs sin repartition.
Salida: docs/evidencias/optimizaciones.csv

Uso: python -m latam_ops.benchmark.optimizaciones --mes 2024-06
"""

from __future__ import annotations

import argparse
import csv

from pyspark.sql import Window
from pyspark.sql import functions as F

from latam_ops.benchmark.metricas_spark import medir
from latam_ops.comun.config import cargar_yaml, lake, ruta_repo
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.sesion_spark import crear_sesion


def _accion(df) -> None:
    df.write.format("noop").mode("overwrite").save()


def ejecutar(mes: str) -> list[dict]:
    lk = lake()
    spark = crear_sesion("optimizaciones")
    anio, m = map(int, mes.split("-"))
    resultados: list[dict] = []
    with registrar_corrida("bench_optimizaciones") as info:
        vuelos = spark.read.parquet(lk.ruta("silver_vuelos"))
        aeros = spark.read.parquet(lk.ruta("gold_dim_aeropuerto"))
        # O1
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        with medir(spark) as r:
            _accion(vuelos.join(aeros, vuelos.origen_icao == aeros.icao))
        resultados.append({"optimizacion": "O1", "variante": "sort_merge_join", **r})
        with medir(spark) as r:
            _accion(vuelos.join(F.broadcast(aeros), vuelos.origen_icao == aeros.icao))
        resultados.append({"optimizacion": "O1", "variante": "broadcast_join", **r})
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        # O2
        with medir(spark) as r:
            _accion(spark.read.parquet(lk.ruta("silver_vuelos")).filter(
                F.date_format("partida_prevista_local", "yyyy-MM") == mes).groupBy("origen_icao").agg(F.avg("demora_salida_min")))
        resultados.append({"optimizacion": "O2", "variante": "lectura_completa_filtro_tardio", **r})
        with medir(spark) as r:
            _accion(spark.read.parquet(lk.ruta("silver_vuelos")).where((F.col("year") == anio) & (F.col("month") == m))
                    .select("origen_icao", "demora_salida_min").groupBy("origen_icao").agg(F.avg("demora_salida_min")))
        resultados.append({"optimizacion": "O2", "variante": "poda_particiones_y_columnas", **r})
        # O3
        feats = spark.read.parquet(lk.ruta("gold_features"))
        aggs = [feats.groupBy("origen_icao").agg(F.avg("y_demora15")), feats.groupBy("hora_local").agg(F.avg("y_demora15")),
                feats.groupBy("o_categoria_vuelo").agg(F.avg("y_demora15"))]
        with medir(spark) as r:
            for a in aggs:
                _accion(a)
        resultados.append({"optimizacion": "O3", "variante": "sin_cache", **r})
        feats.cache().count()
        aggs = [feats.groupBy("origen_icao").agg(F.avg("y_demora15")), feats.groupBy("hora_local").agg(F.avg("y_demora15")),
                feats.groupBy("o_categoria_vuelo").agg(F.avg("y_demora15"))]
        with medir(spark) as r:
            for a in aggs:
                _accion(a)
        resultados.append({"optimizacion": "O3", "variante": "con_cache", **r})
        feats.unpersist()
        # O4
        w = Window.partitionBy("empresa_icao", "numero_voo", "origen_icao").orderBy("partida_prevista_utc")
        with medir(spark) as r:
            _accion(vuelos.withColumn("lag", F.lag("demora_salida_min").over(w)))
        resultados.append({"optimizacion": "O4", "variante": "ventana_sin_repartition", **r})
        with medir(spark) as r:
            _accion(vuelos.repartition("empresa_icao", "numero_voo", "origen_icao").withColumn("lag", F.lag("demora_salida_min").over(w)))
        resultados.append({"optimizacion": "O4", "variante": "ventana_con_repartition", **r})
        salida = ruta_repo(cargar_yaml("pipeline")["evidencias"]["dir_local"], "optimizaciones.csv")
        salida.parent.mkdir(parents=True, exist_ok=True)
        with open(salida, "w", newline="", encoding="utf-8") as f:
            w_csv = csv.DictWriter(f, fieldnames=list(resultados[0].keys()))
            w_csv.writeheader()
            w_csv.writerows(resultados)
        info["detalle"] = f"{len(resultados)} mediciones"
    return resultados


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Optimizaciones O1-O4")
    p.add_argument("--mes", default="2024-06")
    ejecutar(p.parse_args(argv).mes)


if __name__ == "__main__":
    main()
