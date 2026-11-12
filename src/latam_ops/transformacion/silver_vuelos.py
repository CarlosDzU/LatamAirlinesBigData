"""bronze/anac_vra -> silver/vuelos: tipado, UTC, demora de salida, etiquetas y reglas de calidad.

Uso: python -m latam_ops.transformacion.silver_vuelos --desde 2023-01 --hasta 2025-12
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from latam_ops.comun.calidad import Regla, pct, publicar_reporte
from latam_ops.comun.config import cargar_yaml, empresas_latam, lake, periodo_meses
from latam_ops.comun.contratos import aplicar_contrato, esquema, validar_no_nulos
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger, run_id
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.transformacion.comunes import argumentos, escribir_particionado

_log = obtener_logger("silver_vuelos")
_NULOS_LITERALES = ["", "null", "NULL", "N/A", "nan"]
CLAVE_NATURAL = ["empresa_icao", "numero_voo", "origen_icao", "partida_prevista_local"]


def leer_bronze_vra(spark: SparkSession, rutas: list[str]) -> DataFrame:
    """Lee los CSV VRA con el esquema del contrato (todo string) y descarta la línea 'Atualizado em' y el encabezado."""
    df = (
        spark.read.option("sep", ";").option("header", "false").option("quote", '"')
        .option("encoding", "UTF-8").option("mode", "PERMISSIVE")
        .schema(esquema("bronze", "anac_vra")).csv(rutas)
        .withColumn("archivo_origen", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
    )
    primera = F.regexp_replace(F.col("empresa_icao"), "^﻿", "")
    return df.where(~primera.startswith("Atualizado") & (primera != "ICAO Empresa Aérea"))


def _limpiar(col: str) -> F.Column:
    c = F.trim(F.col(col))
    return F.when(c.isin(_NULOS_LITERALES), None).otherwise(c)


def transformar_vuelos(bronze: DataFrame, empresas: list[str], zona_horaria: str,
                       demora_min: float, demora_max: float) -> DataFrame:
    """Aplica limpieza y derivaciones. Función pura (sin E/S) para poder probarla con filas reales."""
    fmt = "yyyy-MM-dd HH:mm:ss"
    df = bronze.select(
        *[_limpiar(c).alias(c) for c in ["empresa_icao", "numero_voo", "codigo_di", "tipo_linha", "origen_icao",
                                         "destino_icao", "partida_prevista", "partida_real", "chegada_prevista",
                                         "chegada_real", "situacao_voo", "codigo_justificativa"]],
        "archivo_origen",
    )
    df = (
        df.withColumn("empresa_icao", F.upper("empresa_icao"))
        .withColumn("origen_icao", F.upper("origen_icao"))
        .withColumn("destino_icao", F.upper("destino_icao"))
        .withColumn("situacao_voo", F.upper("situacao_voo"))
        # Desde oct-2025 algunas filas traen fracciones de segundo ("07:55:00.100000000"): se cortan a 19 caracteres.
        .withColumn("partida_prevista_local", F.to_timestamp(F.substring("partida_prevista", 1, 19), fmt))
        .withColumn("partida_real_local", F.to_timestamp(F.substring("partida_real", 1, 19), fmt))
        .withColumn("chegada_prevista_local", F.to_timestamp(F.substring("chegada_prevista", 1, 19), fmt))
        .withColumn("chegada_real_local", F.to_timestamp(F.substring("chegada_real", 1, 19), fmt))
    )
    for base in ["partida_prevista", "partida_real", "chegada_prevista", "chegada_real"]:
        df = df.withColumn(f"{base}_utc", F.to_utc_timestamp(f"{base}_local", zona_horaria))
    # Deduplicación por clave natural: se prefiere la fila con horario real informado.
    w = Window.partitionBy(*CLAVE_NATURAL).orderBy(F.col("partida_real_utc").asc_nulls_last(), F.col("archivo_origen").desc())
    df = df.where(F.col("partida_prevista_local").isNotNull() & F.col("empresa_icao").isNotNull()
                  & F.col("origen_icao").isNotNull() & F.col("destino_icao").isNotNull() & F.col("numero_voo").isNotNull())
    df = df.withColumn("_n", F.row_number().over(w)).where("_n = 1").drop("_n")
    demora = (F.unix_timestamp("partida_real_utc") - F.unix_timestamp("partida_prevista_utc")) / 60.0
    demora_lleg = (F.unix_timestamp("chegada_real_utc") - F.unix_timestamp("chegada_prevista_utc")) / 60.0
    cancelado = F.col("situacao_voo") == "CANCELADO"
    df = (
        df.withColumn("cancelado", F.coalesce(cancelado, F.lit(False)))
        .withColumn("demora_salida_min", F.when(~F.col("cancelado"), demora))
        .withColumn("demora_llegada_min", F.when(~F.col("cancelado"), demora_lleg))
        .withColumn("es_atipico", F.coalesce((F.col("demora_salida_min") < demora_min) | (F.col("demora_salida_min") > demora_max), F.lit(False)))
        .withColumn("es_latam", F.col("empresa_icao").isin(empresas))
    )
    etiqueta_valida = (~F.col("cancelado")) & F.col("demora_salida_min").isNotNull()
    df = (
        df.withColumn("y_demora15", F.when(etiqueta_valida, (F.col("demora_salida_min") >= 15).cast("int")))
        .withColumn("y_demora60", F.when(etiqueta_valida, (F.col("demora_salida_min") >= 60).cast("int")))
        .withColumn("y_clase", F.when(etiqueta_valida, F.col("y_demora15") + F.col("y_demora60")))
        .withColumn("id_vuelo", F.substring(F.sha2(F.concat_ws("|", "empresa_icao", "numero_voo", "origen_icao",
                                                               F.col("partida_prevista_local").cast("string")), 256), 1, 24))
        .withColumn("year", F.year("partida_prevista_local"))
        .withColumn("month", F.month("partida_prevista_local"))
        .withColumn("run_id", F.lit(run_id()))
    )
    return df


def reglas_calidad(bronze_n: int, silver: DataFrame) -> list[Regla]:
    """Reglas Q1..Q10 sobre silver/vuelos (Q1, Q3 y Q10 son críticas)."""
    m = silver.agg(
        F.count("*").alias("n"),
        F.sum(((F.col("situacao_voo") == "REALIZADO") & F.col("partida_real_utc").isNull()).cast("long")).alias("realizado_sin_real"),
        F.sum((F.col("situacao_voo") == "REALIZADO").cast("long")).alias("realizados"),
        F.sum(F.col("es_atipico").cast("long")).alias("atipicos"),
        F.sum(F.col("cancelado").cast("long")).alias("cancelados"),
        F.sum(((~F.col("origen_icao").rlike("^[A-Z0-9]{4}$")) | (~F.col("destino_icao").rlike("^[A-Z0-9]{4}$"))).cast("long")).alias("icao_invalidos"),
        F.sum(F.col("es_latam").cast("long")).alias("latam"),
        F.sum(F.col("y_demora15")).alias("demoras15"),
        F.count("y_demora15").alias("con_etiqueta"),
    ).first()
    n = m["n"] or 0
    duplicados = silver.groupBy(*CLAVE_NATURAL).count().where("count > 1").count()
    nulos = validar_no_nulos(silver, "silver", "vuelos")
    descartadas = max(bronze_n - n, 0)
    return [
        Regla("Q1", "filas en silver/vuelos", n, "> 0", n > 0, critica=True),
        Regla("Q2", "% filas bronze descartadas (sin fecha prevista o duplicadas)", pct(descartadas, bronze_n), "<= 5", pct(descartadas, bronze_n) <= 5),
        Regla("Q3", "duplicados por clave natural tras deduplicar", duplicados, "= 0", duplicados == 0, critica=True),
        Regla("Q4", "% REALIZADO sin partida real", pct(m["realizado_sin_real"] or 0, m["realizados"] or 0), "<= 2", pct(m["realizado_sin_real"] or 0, m["realizados"] or 0) <= 2),
        Regla("Q5", "% demoras atípicas fuera de [-60, 1440] min", pct(m["atipicos"] or 0, n), "<= 1", pct(m["atipicos"] or 0, n) <= 1),
        Regla("Q7", "% vuelos cancelados (informativa)", pct(m["cancelados"] or 0, n), "informativa", True),
        Regla("Q8", "% códigos ICAO de aeródromo inválidos", pct(m["icao_invalidos"] or 0, n), "<= 0.5", pct(m["icao_invalidos"] or 0, n) <= 0.5),
        Regla("Q9", "% vuelos del grupo LATAM (informativa)", pct(m["latam"] or 0, n), "informativa", True),
        Regla("Q10", "nulos en columnas obligatorias del contrato", sum(nulos.values()), "= 0", sum(nulos.values()) == 0, critica=True, detalle=str(nulos)),
        Regla("QB", "% salidas con demora >= 15 min (línea base, informativa)", pct(m["demoras15"] or 0, m["con_etiqueta"] or 0), "informativa", True),
    ]


def rutas_bronze(lk, desde: str, hasta: str) -> list[str]:
    """Rutas de los meses pedidos en bronze/anac_vra."""
    return [f"{lk.ruta('bronze_vra')}/year={a}/month={m:02d}/" for a, m in periodo_meses(desde, hasta)]


def ejecutar(desde: str, hasta: str, spark: SparkSession | None = None) -> dict:
    """Ejecuta el job para un intervalo de meses."""
    cfg = cargar_yaml("pipeline")
    lk = lake()
    spark = spark or crear_sesion("silver_vuelos")
    with registrar_corrida("silver_vuelos") as info:
        from latam_ops.comun.almacen import Almacen

        alm = Almacen(lk)
        rutas = [r for r, (a, m) in zip(rutas_bronze(lk, desde, hasta), periodo_meses(desde, hasta), strict=True)
                 if alm.listar(f"{lk.relativa('bronze_vra')}/year={a}/month={m:02d}/")]
        if not rutas:
            raise FileNotFoundError(f"No hay archivos VRA en bronze para {desde}..{hasta}")
        bronze = leer_bronze_vra(spark, rutas).cache()
        bronze_n = bronze.count()
        silver = transformar_vuelos(bronze, empresas_latam(), cfg["zona_horaria_vra"],
                                    cfg["calidad"]["demora_min"], cfg["calidad"]["demora_max"])
        silver = aplicar_contrato(silver, "silver", "vuelos").cache()
        malas = silver.where(F.col("es_atipico") | ~F.col("origen_icao").rlike("^[A-Z0-9]{4}$")
                             | ((F.col("situacao_voo") == "REALIZADO") & F.col("partida_real_utc").isNull())).count()
        n_silver = silver.count()
        reporte = publicar_reporte(lk, "silver_vuelos", reglas_calidad(bronze_n, silver),
                                   {"desde": desde, "hasta": hasta, "filas_bronze": bronze_n,
                                    "pct_filas_ok": pct(n_silver - malas, n_silver)})
        escribir_particionado(silver, lk.ruta("silver_vuelos"), ["year", "month"])
        info["filas_entrada"], info["filas_salida"] = bronze_n, silver.count()
        info["detalle"] = f"{desde}..{hasta}"
        bronze.unpersist()
        silver.unpersist()
    return reporte


def main(argv: list[str] | None = None) -> None:
    a = argumentos("bronze/anac_vra -> silver/vuelos", argv)
    ejecutar(a.desde, a.hasta)


if __name__ == "__main__":
    main()
