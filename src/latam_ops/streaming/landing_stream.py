"""Demo de punta a punta con Spark Structured Streaming (trigger availableNow).

Un archivo VRA nuevo que cae en bronze/landing/anac_vra/ se procesa solo: se transforma a silver/vuelos
(sobrescribiendo únicamente su mes) y luego se recalculan gold (features, predicciones, estrella y KPI)
para los meses afectados. El checkpoint garantiza que cada archivo se procese una sola vez.

Uso: python -m latam_ops.streaming.landing_stream
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from latam_ops.comun.config import cargar_yaml, empresas_latam, lake
from latam_ops.comun.contratos import aplicar_contrato, esquema
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.transformacion.comunes import escribir_particionado
from latam_ops.transformacion.silver_vuelos import transformar_vuelos

_log = obtener_logger("landing_stream")


def ejecutar(continuar_gold: bool = True) -> list[str]:
    cfg = cargar_yaml("pipeline")
    lk = lake()
    spark = crear_sesion("landing_stream")
    origen = lk.ruta(cfg["lake"]["landing"] + "/anac_vra")
    checkpoint = lk.ruta(cfg["lake"]["landing"] + "/_checkpoints/anac_vra")
    meses: set[str] = set()

    def procesar_lote(lote: DataFrame, id_lote: int) -> None:
        primera = F.regexp_replace(F.col("empresa_icao"), "^﻿", "")
        lote = lote.where(~primera.startswith("Atualizado") & (primera != "ICAO Empresa Aérea"))
        if lote.limit(1).count() == 0:
            return
        silver = transformar_vuelos(lote, empresas_latam(), cfg["zona_horaria_vra"],
                                    cfg["calidad"]["demora_min"], cfg["calidad"]["demora_max"])
        silver = aplicar_contrato(silver, "silver", "vuelos").cache()
        nuevos = [f"{r['year']}-{r['month']:02d}" for r in silver.select("year", "month").distinct().collect()]
        escribir_particionado(silver, lk.ruta("silver_vuelos"), ["year", "month"])
        meses.update(nuevos)
        _log.info(f"lote {id_lote}: {silver.count()} vuelos -> silver, meses {nuevos}")
        silver.unpersist()

    with registrar_corrida("landing_stream") as info:
        flujo = (
            spark.readStream.schema(esquema("bronze", "anac_vra")).option("sep", ";").option("header", "false")
            .option("encoding", "UTF-8").csv(origen)
            .withColumn("archivo_origen", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
        )
        consulta = flujo.writeStream.foreachBatch(procesar_lote).option("checkpointLocation", checkpoint).trigger(
            availableNow=True).start()
        consulta.awaitTermination()
        info["detalle"] = f"meses nuevos: {sorted(meses)}"
    if continuar_gold and meses:
        from latam_ops.orquestacion.ejecutar_paso import ejecutar_cadena_gold

        for mes in sorted(meses):
            ejecutar_cadena_gold(mes, mes)
    return sorted(meses)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Structured Streaming sobre bronze/landing (demo)")
    p.add_argument("--solo-silver", action="store_true")
    ejecutar(not p.parse_args(argv).solo_silver)


if __name__ == "__main__":
    main()
