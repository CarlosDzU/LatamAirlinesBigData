"""Elección del umbral de alerta por costo esperado: costo = C_FN * FN + C_FP * FP.

El umbral teórico para un clasificador calibrado es tau* = C_FP / (C_FP + C_FN); aquí se busca
empíricamente en VALIDACIÓN para cada razón C_FN/C_FP y se reporta la tabla de sensibilidad.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def curva(df: DataFrame, objetivo: str, col_score: str = "prob", pasos: int = 100) -> list[dict]:
    """VP/FP/FN/VN para umbrales 0.01..0.99 con una sola agregación (histograma acumulado)."""
    b = df.select(F.least(F.floor(F.col(col_score) * pasos), F.lit(pasos - 1)).cast("int").alias("b"),
                  F.col(objetivo).cast("int").alias("y"))
    hist = {r["b"]: (r["pos"], r["neg"]) for r in
            b.groupBy("b").agg(F.sum("y").alias("pos"), F.sum(1 - F.col("y")).alias("neg")).collect()}
    total_pos = sum(p for p, _ in hist.values())
    total_neg = sum(n for _, n in hist.values())
    filas, vp, fp = [], total_pos, total_neg
    for i in range(pasos):
        # umbral i/pasos: positivos predichos = cubetas >= i
        if i > 0:
            p, n = hist.get(i - 1, (0, 0))
            vp, fp = vp - p, fp - n
        filas.append({"umbral": i / pasos, "vp": vp, "fp": fp, "fn": total_pos - vp, "vn": total_neg - fp})
    return filas


def elegir(curva_valid: list[dict], razones: list[float], costo_fp: float = 1.0) -> list[dict]:
    """Umbral de mínimo costo para cada razón C_FN/C_FP."""
    salida = []
    for r in razones:
        cfn = r * costo_fp
        mejor = min(curva_valid[1:], key=lambda f: cfn * f["fn"] + costo_fp * f["fp"])
        salida.append({"razon_cfn_cfp": r, "umbral": mejor["umbral"], "umbral_teorico": round(costo_fp / (costo_fp + cfn), 4),
                       "costo": cfn * mejor["fn"] + costo_fp * mejor["fp"], "vp": mejor["vp"], "fp": mejor["fp"],
                       "fn": mejor["fn"], "vn": mejor["vn"]})
    return salida
