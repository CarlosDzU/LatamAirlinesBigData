"""Prueba de escalabilidad del pipeline completo (silver -> gold features), no de lecturas repetidas (sección 3.7).

Escalones: E1 un mes (2024-01) · E2 un año (2024) · E3 tres años reales (2023-2025) · E4 réplica 10x de 2024 ·
E5 réplica 100x de 2024 · E6 una sola máquina con pandas sobre 2024 (ver una_maquina.py).
Las réplicas se escriben solo en scale-test/ (expiran a los 7 días por lifecycle) desplazando las fechas por
bloques de años y agregando sufijo al número de vuelo; nunca se usan para EDA ni ML.
Salida: docs/evidencias/escalabilidad.csv y escalabilidad.png (log-log tiempo vs filas).

Uso: python -m latam_ops.benchmark.escalabilidad --escalon E4 [--nodos 2]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from latam_ops.comun.config import cargar_yaml, lake, ruta_repo
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.transformacion.comunes import meses_str
from latam_ops.transformacion.gold_features import calcular_tasa_hist, construir_features

_log = obtener_logger("escalabilidad")
ESCALONES = {"E1": ("2024-01", "2024-01", 1), "E2": ("2024-01", "2024-12", 1), "E3": ("2023-01", "2025-12", 1),
             "E4": ("2024-01", "2024-12", 10), "E5": ("2024-01", "2024-12", 100)}
_TS_VUELOS = ["partida_prevista_local", "partida_prevista_utc", "partida_real_utc", "chegada_prevista_utc", "chegada_real_utc"]


def replicar(vuelos: DataFrame, clima: DataFrame, factor: int) -> tuple[DataFrame, DataFrame]:
    """Réplica k = 0..factor-1 desplazando fechas k*10 años (no se mezclan rotaciones ni claves)."""
    spark = vuelos.sparkSession
    ks = spark.range(factor).withColumnRenamed("id", "k")
    v = vuelos.crossJoin(F.broadcast(ks))
    for c in _TS_VUELOS:
        v = v.withColumn(c, F.expr(f"{c} + make_interval(cast(k * 10 as int), 0, 0, 0, 0, 0, 0)"))
    v = (v.withColumn("numero_voo", F.concat("numero_voo", F.lit("_r"), F.col("k").cast("string")))
         .withColumn("id_vuelo", F.concat("id_vuelo", F.lit("_r"), F.col("k").cast("string")))
         .withColumn("year", F.year("partida_prevista_local")).drop("k"))
    c = (clima.crossJoin(F.broadcast(ks.withColumnRenamed("k", "r")))
         .withColumn("k", F.expr("k + make_interval(cast(r * 10 as int), 0, 0, 0, 0, 0, 0)"))
         .withColumn("obs_utc", F.expr("obs_utc + make_interval(cast(r * 10 as int), 0, 0, 0, 0, 0, 0)"))
         .withColumn("year", F.year("k")).drop("r"))
    return v, c


def _bytes(spark: SparkSession, ruta: str) -> int:
    jvm = spark._jvm
    ruta_h = jvm.org.apache.hadoop.fs.Path(ruta)
    fs = ruta_h.getFileSystem(spark._jsc.hadoopConfiguration())
    return int(fs.getContentSummary(ruta_h).getLength()) if fs.exists(ruta_h) else 0


def _pico_memoria(spark: SparkSession) -> int:
    url = spark.sparkContext.uiWebUrl
    try:
        with urllib.request.urlopen(f"{url}/api/v1/applications/{spark.sparkContext.applicationId}/executors", timeout=10) as r:
            ejecutores = json.loads(r.read())
        return max(int((e.get("peakMemoryMetrics") or {}).get("JVMHeapMemory", 0)) for e in ejecutores)
    except (OSError, ValueError):
        return 0


def ejecutar(escalon: str, nodos: int) -> dict:
    cfg = cargar_yaml("pipeline")
    lk = lake()
    spark = crear_sesion(f"escalabilidad_{escalon}")
    desde, hasta, factor = ESCALONES[escalon]
    base = f"{lk.base}/{cfg['lake']['scale_test']}/{escalon}"
    fila = {"escalon": escalon, "nodos": nodos, "factor": factor, "desde": desde, "hasta": hasta, "estado": "ok"}
    with registrar_corrida(f"bench_escalabilidad_{escalon}") as info:
        ini, fin = int(desde.replace("-", "")), int(hasta.replace("-", ""))
        en_rango = (F.col("year") * 100 + F.col("month")).between(ini, fin)
        vuelos = spark.read.parquet(lk.ruta("silver_vuelos")).where(en_rango)
        clima = spark.read.parquet(lk.ruta("silver_clima_horario")).where(en_rango)
        if factor > 1:
            t0 = time.time()
            v, c = replicar(vuelos, clima, factor)
            v.repartition("year", "month").write.mode("overwrite").partitionBy("year", "month").parquet(f"{base}/silver_vuelos")
            c.repartition("year", "month").write.mode("overwrite").partitionBy("year", "month").parquet(f"{base}/clima_horario")
            fila["segundos_replica"] = round(time.time() - t0, 2)
            vuelos, clima = spark.read.parquet(f"{base}/silver_vuelos"), spark.read.parquet(f"{base}/clima_horario")
            fila["bytes_entrada"] = _bytes(spark, f"{base}/silver_vuelos") + _bytes(spark, f"{base}/clima_horario")
        else:
            fila["bytes_entrada"] = sum(
                _bytes(spark, f"{lk.ruta(t)}/year={mes[:4]}/month={int(mes[5:])}")
                for t in ("silver_vuelos", "silver_clima_horario") for mes in meses_str(desde, hasta))
        fila["filas_entrada"] = vuelos.count()
        t0 = time.time()
        try:
            feats = construir_features(vuelos, clima, cfg, calcular_tasa_hist(vuelos, cfg["split"]["train_fin"]))
            feats.write.mode("overwrite").parquet(f"{base}/gold_features")
            fila["filas_salida"] = spark.read.parquet(f"{base}/gold_features").count()
        except Exception as exc:  # se registra el punto de quiebre (p. ej. OOM) en lugar de ocultarlo
            fila["estado"] = "OOM" if "OutOfMemory" in str(exc) or "Java heap" in str(exc) else "error"
            fila["detalle"] = str(exc)[:300]
        fila["segundos_pipeline"] = round(time.time() - t0, 2)
        fila["pico_heap_executor_bytes"] = _pico_memoria(spark)
        info["filas_entrada"], info["filas_salida"] = fila["filas_entrada"], fila.get("filas_salida")
    guardar(fila)
    return fila


def guardar(fila: dict) -> None:
    """Agrega la medición al CSV y regenera el gráfico log-log."""
    directorio = ruta_repo(cargar_yaml("pipeline")["evidencias"]["dir_local"])
    directorio.mkdir(parents=True, exist_ok=True)
    archivo = directorio / "escalabilidad.csv"
    campos = ["escalon", "nodos", "factor", "desde", "hasta", "estado", "filas_entrada", "filas_salida", "bytes_entrada",
              "segundos_replica", "segundos_pipeline", "pico_heap_executor_bytes", "detalle"]
    nuevo = not archivo.exists()
    with open(archivo, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
        if nuevo:
            w.writeheader()
        w.writerow(fila)
    graficar(archivo, directorio / "escalabilidad.png")


def graficar(csv_path, png_path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd

    todo = pd.read_csv(csv_path)
    df = todo[todo["estado"] == "ok"].dropna(subset=["filas_entrada", "segundos_pipeline"])
    fallas = todo[todo["estado"] != "ok"].dropna(subset=["filas_entrada", "segundos_pipeline"])
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for _, r in fallas.iterrows():
        ax.scatter(r["filas_entrada"], r["segundos_pipeline"], marker="x", s=60, color="crimson")
        ax.annotate(f"{r['escalon']} ({r['estado']})", (r["filas_entrada"], r["segundos_pipeline"]), fontsize=8,
                    xytext=(-60, 6), textcoords="offset points", color="crimson")
    for (nodos, grupo) in df.groupby("nodos"):
        g = grupo.sort_values("filas_entrada")
        etiqueta = "1 máquina (pandas)" if nodos == 0 else f"Spark, {nodos} nodo(s)"
        ax.plot(g["filas_entrada"], g["segundos_pipeline"], marker="o", label=etiqueta)
        for _, r in g.iterrows():
            ax.annotate(r["escalon"], (r["filas_entrada"], r["segundos_pipeline"]), fontsize=8, xytext=(4, 4), textcoords="offset points")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Vuelos procesados (escala log)")
    ax.set_ylabel("Tiempo del pipeline silver→gold, s (escala log)")
    ax.set_title("Escalabilidad del pipeline de features")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(png_path, dpi=150)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Escalabilidad E1-E5")
    p.add_argument("--escalon", required=True, choices=sorted(ESCALONES))
    p.add_argument("--nodos", type=int, default=int(os.environ.get("LATAM_NODOS", "1")))
    a = p.parse_args(argv)
    ejecutar(a.escalon, a.nodos)


if __name__ == "__main__":
    main()
