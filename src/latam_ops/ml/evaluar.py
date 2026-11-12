"""Métricas de clasificación: AUC-ROC, AUC-PR, matriz de confusión al umbral y métricas por segmento."""

from __future__ import annotations

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def con_probabilidad(pred: DataFrame, col_prob: str = "probability", salida: str = "prob") -> DataFrame:
    """Extrae P(clase=1) del vector de probabilidades de Spark ML."""
    return pred.withColumn(salida, vector_to_array(col_prob)[1])


def aucs(df: DataFrame, objetivo: str, col_score: str = "prob") -> dict[str, float]:
    """AUC-ROC y AUC-PR usando el score continuo."""
    d = df.where(F.col(col_score).isNotNull()).select(F.col(col_score).cast("double").alias("score"),
                                                      F.col(objetivo).cast("double").alias("label"))
    if d.select("label").distinct().count() < 2:
        return {"auc_roc": float("nan"), "auc_pr": float("nan")}
    ev = BinaryClassificationEvaluator(rawPredictionCol="score", labelCol="label")
    return {"auc_roc": round(ev.setMetricName("areaUnderROC").evaluate(d), 4),
            "auc_pr": round(ev.setMetricName("areaUnderPR").evaluate(d), 4)}


def matriz(df: DataFrame, objetivo: str, umbral: float, col_score: str = "prob") -> dict[str, float]:
    """Matriz de confusión y precisión/recall/F1 de la clase positiva (alerta) al umbral dado."""
    pred = (F.col(col_score) >= umbral).cast("int")
    y = F.col(objetivo).cast("int")
    m = df.agg(
        F.sum(((pred == 1) & (y == 1)).cast("long")).alias("vp"), F.sum(((pred == 1) & (y == 0)).cast("long")).alias("fp"),
        F.sum(((pred == 0) & (y == 1)).cast("long")).alias("fn"), F.sum(((pred == 0) & (y == 0)).cast("long")).alias("vn"),
    ).first()
    vp, fp, fn, vn = (int(m[k] or 0) for k in ("vp", "fp", "fn", "vn"))
    precision = vp / (vp + fp) if vp + fp else 0.0
    recall = vp / (vp + fn) if vp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    exactitud = (vp + vn) / max(vp + fp + fn + vn, 1)
    return {"umbral": round(umbral, 4), "vp": vp, "fp": fp, "fn": fn, "vn": vn, "precision": round(precision, 4),
            "recall": round(recall, 4), "f1": round(f1, 4), "exactitud": round(exactitud, 4),
            "tasa_alerta": round((vp + fp) / max(vp + fp + fn + vn, 1), 4)}


def por_segmento(df: DataFrame, objetivo: str, umbral: float, top_n: int, col_score: str = "prob") -> list[dict]:
    """Métricas por segmento: aeródromos top-N por volumen vs resto, y por empresa."""
    top = [r["origen_icao"] for r in df.groupBy("origen_icao").count().orderBy(F.desc("count")).limit(top_n).collect()]
    salida = []
    seg = df.withColumn("segmento", F.when(F.col("origen_icao").isin(top), "top").otherwise("resto"))
    for nombre in ("top", "resto"):
        parte = seg.where(F.col("segmento") == nombre)
        if parte.limit(1).count():
            salida.append({"segmento": f"aeropuertos_{nombre}{top_n if nombre == 'top' else ''}",
                           **aucs(parte, objetivo, col_score), **matriz(parte, objetivo, umbral, col_score)})
    for fila in df.select("empresa_icao").distinct().collect():
        parte = df.where(F.col("empresa_icao") == fila["empresa_icao"])
        if parte.count() >= 200:
            salida.append({"segmento": f"empresa_{fila['empresa_icao']}", **aucs(parte, objetivo, col_score),
                           **matriz(parte, objetivo, umbral, col_score)})
    return salida
