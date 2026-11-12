"""Medición de un bloque: tiempo de pared + métricas de stages desde la API REST de la Spark UI."""

from __future__ import annotations

import json
import time
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager

from pyspark.sql import SparkSession

_CAMPOS = ("executorRunTime", "shuffleReadBytes", "shuffleWriteBytes", "memoryBytesSpilled", "diskBytesSpilled", "inputBytes")


def _stages(spark: SparkSession) -> list[dict]:
    url = spark.sparkContext.uiWebUrl
    if not url:
        return []
    app = spark.sparkContext.applicationId
    try:
        with urllib.request.urlopen(f"{url}/api/v1/applications/{app}/stages?status=complete", timeout=10) as r:
            return json.loads(r.read())
    except OSError:
        return []


@contextmanager
def medir(spark: SparkSession) -> Iterator[dict]:
    """Mide el bloque: segundos y suma de métricas de los stages completados dentro del bloque."""
    antes = {s["stageId"] for s in _stages(spark)}
    res: dict = {}
    inicio = time.time()
    yield res
    res["segundos"] = round(time.time() - inicio, 3)
    nuevos = [s for s in _stages(spark) if s["stageId"] not in antes]
    for c in _CAMPOS:
        res[c] = sum(int(s.get(c, 0) or 0) for s in nuevos)
    res["stages"] = len(nuevos)
