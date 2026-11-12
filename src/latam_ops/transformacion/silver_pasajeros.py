"""bronze/anac_estatisticos -> silver/pasajeros_ruta_mes: pasajeros y asientos por despegue de LATAM.

VRA trae cada vuelo pero no cuántos pasajeros lleva. Los Dados Estatísticos traen, por empresa, ruta y mes,
pasajeros pagos y gratis, asientos y despegues. El promedio por despegue sirve para estimar los pasajeros
de cada vuelo de la misma empresa, ruta y mes (en el valor de negocio: minutos-pasajero y umbrales de la
Resolución ANAC 400).

Uso: python -m latam_ops.transformacion.silver_pasajeros [--desde 2023-01 --hasta 2025-12]
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from latam_ops.comun.almacen import Almacen
from latam_ops.comun.config import empresas_latam, lake
from latam_ops.comun.contratos import aplicar_contrato, esquema, validar_no_nulos
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.transformacion.comunes import argumentos, escribir_particionado

_log = obtener_logger("silver_pasajeros")


def leer_bronze(spark: SparkSession, rutas: list[str]) -> DataFrame:
    """Lee el CSV con el esquema del contrato; descarta 'Atualizado em' y el encabezado."""
    df = (spark.read.option("sep", ";").option("header", "false").option("quote", '"').option("encoding", "UTF-8")
          .schema(esquema("bronze", "anac_estatisticos")).csv(rutas))
    primera = F.regexp_replace(F.col("empresa_sigla"), "^﻿", "")
    return df.where(~primera.startswith("Atualizado") & (primera != "EMPRESA_SIGLA"))


def _numero(col: str) -> F.Column:
    """Los números vienen como texto con coma decimal; vacío = 0 (la ANAC deja vacío lo que no aplica)."""
    return F.coalesce(F.regexp_replace(F.trim(F.col(col)), ",", ".").cast("double"), F.lit(0.0))


def transformar(bronze: DataFrame, empresas: list[str], desde: str, hasta: str) -> DataFrame:
    """Filtra vuelos regulares de las empresas LATAM en el periodo y agrega por empresa, ruta y mes."""
    a1, a2 = int(desde[:4]), int(hasta[:4])
    d = (bronze.withColumn("empresa_icao", F.upper(F.trim("empresa_sigla")))
         .withColumn("year", F.col("ano").cast("int")).withColumn("month", F.col("mes").cast("int"))
         .where(F.col("empresa_icao").isin(empresas) & F.col("year").between(a1, a2)
                & (F.upper(F.trim("grupo_de_voo")) == "REGULAR"))
         .where(F.format_string("%04d-%02d", "year", "month").between(desde, hasta)))
    agg = (d.groupBy("empresa_icao", F.upper(F.trim("aeroporto_de_origem_sigla")).alias("origen_icao"),
                     F.upper(F.trim("aeroporto_de_destino_sigla")).alias("destino_icao"), "year", "month")
           .agg((F.sum(_numero("passageiros_pagos")) + F.sum(_numero("passageiros_gratis"))).alias("pasajeros"),
                F.sum(_numero("assentos")).alias("asientos"), F.sum(_numero("decolagens")).alias("despegues"))
           .where(F.col("despegues") > 0)
           .withColumn("pax_por_despegue", F.col("pasajeros") / F.col("despegues"))
           .withColumn("asientos_por_despegue", F.col("asientos") / F.col("despegues")))
    return aplicar_contrato(agg, "silver", "pasajeros_ruta_mes")


def ejecutar(desde: str, hasta: str, spark: SparkSession | None = None) -> dict:
    lk = lake()
    spark = spark or crear_sesion("silver_pasajeros")
    with registrar_corrida("silver_pasajeros") as info:
        base = lk.relativa("bronze_estatisticos")
        archivos = sorted(a for a in Almacen(lk).listar(base + "/") if a.endswith(".csv"))
        if not archivos:
            raise RuntimeError("no hay Dados Estatísticos en bronze: correr latam_ops.conectores.estatisticos")
        ultimo = archivos[-1]  # la ANAC reemplaza el archivo completo; se usa la ingesta más reciente
        bronze = leer_bronze(spark, [lk.ruta(ultimo)])
        salida = transformar(bronze, empresas_latam(), desde, hasta)
        validar_no_nulos(salida, "silver", "pasajeros_ruta_mes")
        escribir_particionado(salida, lk.ruta("silver_pasajeros"), ["year", "month"])
        r = spark.read.parquet(lk.ruta("silver_pasajeros")).agg(F.count("*").alias("n"), F.sum("pasajeros").alias("p"),
                                                                  F.sum("despegues").alias("d")).first()
        info["filas_salida"] = int(r["n"])
        info["detalle"] = f"{ultimo}; {int(r['p'] or 0)} pasajeros en {int(r['d'] or 0)} despegues"
        _log.info(info["detalle"])
        return info


def main(argv: list[str] | None = None) -> None:
    a = argumentos("bronze/anac_estatisticos -> silver/pasajeros_ruta_mes", argv)
    ejecutar(a.desde, a.hasta)


if __name__ == "__main__":
    main()
