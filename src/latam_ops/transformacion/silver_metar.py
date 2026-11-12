"""bronze/metar_iem -> silver/metar_obs (METAR crudo parseado) y silver/clima_horario (grilla as-of).

Uso: python -m latam_ops.transformacion.silver_metar --desde 2023-01 --hasta 2025-12
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from latam_ops.comun.calidad import Regla, pct, publicar_reporte
from latam_ops.comun.config import cargar_yaml, lake
from latam_ops.comun.contratos import aplicar_contrato, esquema
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.transformacion.comunes import argumentos, escribir_particionado, filtro_meses
from latam_ops.transformacion.parser_metar import CAMPOS, esquema_spark, parsear_metar

_log = obtener_logger("silver_metar")


def _udf_parser():
    def _parse(texto):
        d = parsear_metar(texto)
        return tuple(d[n] for n, _ in CAMPOS)

    return F.udf(_parse, esquema_spark())


def leer_bronze_metar(spark: SparkSession, rutas: list[str]) -> DataFrame:
    """Lee METAR crudo con esquema explícito (station, valid, metar)."""
    return (spark.read.option("header", "true").option("recursiveFileLookup", "true")
            .schema(esquema("bronze", "metar_iem")).csv(rutas))


def parsear(bronze: DataFrame) -> DataFrame:
    """Parsea el texto y produce metar_obs (deduplicado por estación y hora de observación)."""
    parse = _udf_parser()
    df = (
        bronze.where(F.col("metar").isNotNull() & F.col("valid").isNotNull())
        .withColumn("estacion", F.upper(F.trim("station")))
        .withColumn("obs_utc", F.to_timestamp("valid", "yyyy-MM-dd HH:mm"))
        .where(F.col("obs_utc").isNotNull())
        .withColumn("p", parse(F.col("metar")))
        .select("estacion", "obs_utc", "metar", "p.*")
    )
    # Un reporte por (estación, minuto): se prefiere el rutinario METAR sobre el SPECI.
    w = Window.partitionBy("estacion", "obs_utc").orderBy(F.col("tipo_reporte").asc(), F.col("metar").desc())
    df = df.withColumn("_n", F.row_number().over(w)).where("_n = 1").drop("_n")
    return df.withColumn("year", F.year("obs_utc")).withColumn("month", F.month("obs_utc"))


def construir_clima_horario(obs: DataFrame, ffill_max_horas: int) -> DataFrame:
    """Grilla horaria por estación: el valor de la hora k es la última observación estrictamente anterior a k.

    k = date_trunc('hour', obs_utc) + 1 h, así una observación de las 10:20 alimenta k = 11:00 y nunca k = 10:00.
    Si en la hora no hubo observación, se arrastra la última conocida hasta `ffill_max_horas` horas.
    """
    obs = obs.where("parseado_ok")
    base = obs.withColumn("k", F.date_trunc("hour", "obs_utc") + F.expr("INTERVAL 1 HOUR"))
    w_ult = Window.partitionBy("estacion", "k").orderBy(F.col("obs_utc").desc())
    base = base.withColumn("_n", F.row_number().over(w_ult)).where("_n = 1").drop("_n")
    rango = base.groupBy("estacion").agg(F.min("k").alias("k_min"), F.max("k").alias("k_max"))
    grilla = rango.select("estacion", F.explode(F.sequence("k_min", "k_max", F.expr("INTERVAL 1 HOUR"))).alias("k"))
    campos = ["obs_utc", "viento_kt", "rafaga_kt", "vis_m", "techo_ft", "temp_c", "rocio_c", "qnh_hpa",
              "fen_niebla", "fen_neblina", "fen_lluvia", "fen_tormenta", "tiene_tempo", "categoria_vuelo"]
    base = base.select("estacion", "k", F.struct(*campos).alias("obs"))
    df = grilla.join(base, ["estacion", "k"], "left")
    w = Window.partitionBy("estacion").orderBy("k").rowsBetween(-ffill_max_horas, 0)
    df = df.withColumn("obs", F.last("obs", ignorenulls=True).over(w)).select("estacion", "k", "obs.*")
    w_lag = Window.partitionBy("estacion").orderBy("k")
    df = (
        df.withColumn("horas_desde_obs", (F.unix_timestamp("k") - F.unix_timestamp("obs_utc")) / 3600.0)
        .withColumn("spread_t_td", F.col("temp_c") - F.col("rocio_c"))
        .withColumn("presion_tendencia_3h", F.col("qnh_hpa") - F.lag("qnh_hpa", 3).over(w_lag))
        .withColumn("vis_cambio_3h", (F.col("vis_m") - F.lag("vis_m", 3).over(w_lag)).cast("double"))
        .withColumn("year", F.year("k")).withColumn("month", F.month("k"))
    )
    return df


def ejecutar(desde: str, hasta: str, spark: SparkSession | None = None) -> dict:
    """Procesa los años que cubren el intervalo (los archivos METAR son anuales) y escribe ambas tablas."""
    cfg = cargar_yaml("pipeline")
    lk = lake()
    spark = spark or crear_sesion("silver_metar")
    with registrar_corrida("silver_metar") as info:
        anios = range(int(desde[:4]), int(hasta[:4]) + 1)
        rutas = [f"{lk.ruta('bronze_metar')}/year={a}/" for a in anios]
        from latam_ops.comun.almacen import Almacen

        alm = Almacen(lk)
        rutas = [r for r, a in zip(rutas, anios, strict=True) if alm.listar(f"{lk.relativa('bronze_metar')}/year={a}/")]
        if not rutas:
            raise FileNotFoundError(f"No hay METAR en bronze para {desde}..{hasta}")
        bronze = leer_bronze_metar(spark, rutas)
        n_bronze = bronze.count()
        obs_todo = aplicar_contrato(parsear(bronze), "silver", "metar_obs").cache()
        obs = obs_todo.where(filtro_meses(desde, hasta))
        m = obs.agg(F.count("*").alias("n"), F.sum(F.col("parseado_ok").cast("long")).alias("ok"),
                    F.countDistinct("estacion").alias("estaciones")).first()
        reglas = [
            Regla("M1", "observaciones METAR en silver", m["n"], "> 0", (m["n"] or 0) > 0, critica=True),
            Regla("M2", "% METAR parseados correctamente", pct(m["ok"] or 0, m["n"] or 0), ">= 95", pct(m["ok"] or 0, m["n"] or 0) >= 95),
            Regla("M3", "estaciones con observaciones (informativa)", m["estaciones"], "informativa", True),
        ]
        reporte = publicar_reporte(lk, "silver_metar_obs", reglas, {"desde": desde, "hasta": hasta, "filas_bronze": n_bronze,
                                                                   "pct_filas_ok": pct(m["ok"] or 0, m["n"] or 0)})
        escribir_particionado(obs, lk.ruta("silver_metar"), ["year", "month"])
        # La grilla se arma con todas las observaciones leídas (incluye horas previas al intervalo) y luego se filtra.
        clima = construir_clima_horario(obs_todo, cfg["clima"]["ffill_max_horas"])
        clima = aplicar_contrato(clima, "silver", "clima_horario").where(filtro_meses(desde, hasta))
        escribir_particionado(clima, lk.ruta("silver_clima_horario"), ["year", "month"])
        info["filas_entrada"], info["filas_salida"] = n_bronze, m["n"]
        info["detalle"] = f"{desde}..{hasta}; parseados {pct(m['ok'] or 0, m['n'] or 0)}%"
        obs_todo.unpersist()
    return reporte


def main(argv: list[str] | None = None) -> None:
    a = argumentos("bronze/metar_iem -> silver/metar_obs + silver/clima_horario", argv)
    ejecutar(a.desde, a.hasta)


if __name__ == "__main__":
    main()
