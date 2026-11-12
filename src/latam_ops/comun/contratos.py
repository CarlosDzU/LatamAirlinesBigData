"""Contratos de datos: YAML por tabla -> StructType, validación y proyección de columnas."""

from __future__ import annotations

from functools import cache
from typing import Any

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from latam_ops.comun.config import ruta_repo

_TIPOS: dict[str, T.DataType] = {
    "string": T.StringType(),
    "integer": T.IntegerType(),
    "long": T.LongType(),
    "double": T.DoubleType(),
    "boolean": T.BooleanType(),
    "timestamp": T.TimestampType(),
    "date": T.DateType(),
}


class ErrorContrato(ValueError):
    """El DataFrame no cumple su contrato."""


@cache
def cargar_contrato(zona: str, nombre: str) -> dict[str, Any]:
    """Lee contratos/<zona>/<nombre>.yaml."""
    with open(ruta_repo("contratos", zona, f"{nombre}.yaml"), encoding="utf-8") as f:
        return yaml.safe_load(f)


def esquema(zona: str, nombre: str) -> T.StructType:
    """StructType derivado del contrato (todas las columnas admiten nulos a nivel Spark)."""
    contrato = cargar_contrato(zona, nombre)
    return T.StructType([T.StructField(c["nombre"], _TIPOS[c["tipo"]], True) for c in contrato["columnas"]])


def columnas(zona: str, nombre: str) -> list[str]:
    """Nombres de columnas del contrato en orden."""
    return [c["nombre"] for c in cargar_contrato(zona, nombre)["columnas"]]


def aplicar_contrato(df: DataFrame, zona: str, nombre: str) -> DataFrame:
    """Proyecta y castea el DataFrame al contrato. Falla si falta alguna columna."""
    contrato = cargar_contrato(zona, nombre)
    faltantes = [c["nombre"] for c in contrato["columnas"] if c["nombre"] not in df.columns]
    if faltantes:
        raise ErrorContrato(f"{zona}/{nombre}: faltan columnas {faltantes}")
    return df.select([F.col(c["nombre"]).cast(_TIPOS[c["tipo"]]).alias(c["nombre"]) for c in contrato["columnas"]])


def validar_no_nulos(df: DataFrame, zona: str, nombre: str) -> dict[str, int]:
    """Cuenta nulos en las columnas declaradas 'nulo: false'. Devuelve {columna: nulos}."""
    obligatorias = [c["nombre"] for c in cargar_contrato(zona, nombre)["columnas"] if c.get("nulo") is False]
    if not obligatorias:
        return {}
    fila = df.select([F.sum(F.col(c).isNull().cast("long")).alias(c) for c in obligatorias]).first()
    return {c: int(fila[c] or 0) for c in obligatorias}
