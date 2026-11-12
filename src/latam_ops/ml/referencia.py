"""Referencia ingenua (modelo trivial): tasa histórica de demora por (origen, hora local) calculada en train.

Cualquier modelo debe superarla; si no, no aporta valor.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from latam_ops.ml.evaluar import aucs, matriz


def evaluar_referencia(particiones: dict[str, DataFrame], objetivo: str) -> dict:
    """Usa tasa_hist_origen_hora (solo train) como score; para y_demora60 se escala por la razón de tasas de train."""
    train = particiones["train"]
    tasa15 = train.agg(F.avg("y_demora15")).first()[0] or 0.0
    tasa_obj = train.agg(F.avg(objetivo)).first()[0] or 0.0
    factor = (tasa_obj / tasa15) if tasa15 else 1.0
    salida = {"modelo": "referencia_tasa_historica", "objetivo": objetivo, "tasa_positiva_train": round(tasa_obj, 4)}
    for nombre in ("valid", "test"):
        df = particiones[nombre].withColumn("prob", F.coalesce(F.col("tasa_hist_origen_hora"), F.lit(tasa15)) * factor)
        salida[nombre] = {**aucs(df, objetivo), **matriz(df, objetivo, tasa_obj)}
    return salida
