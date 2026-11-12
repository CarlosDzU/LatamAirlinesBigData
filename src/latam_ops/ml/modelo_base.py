"""Modelo base: referencia ingenua + regresión logística (Pipeline de Spark ML) para y_demora15.

Uso: python -m latam_ops.ml.modelo_base
"""

from __future__ import annotations

import argparse
import json

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import functions as F

from latam_ops.comun.config import cargar_yaml, lake, ruta_repo
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.ml import umbral as umb
from latam_ops.ml.datasets import cargar_features, con_peso, particionar
from latam_ops.ml.evaluar import aucs, con_probabilidad, matriz
from latam_ops.ml.preparacion import etapas
from latam_ops.ml.referencia import evaluar_referencia
from latam_ops.ml.registro import registrar

_log = obtener_logger("modelo_base")


def entrenar_logistica(train, objetivo: str):
    """Pipeline completo ajustado solo con train (incluye imputación y escalado: sin fuga hacia valid/test)."""
    tasa = train.agg(F.avg(objetivo)).first()[0] or 0.5
    lr = LogisticRegression(featuresCol="features", labelCol=objetivo, weightCol="peso", maxIter=50, regParam=0.01,
                            elasticNetParam=0.0)
    return Pipeline(stages=etapas(escalar=True) + [lr]).fit(con_peso(train, objetivo, tasa))


def ejecutar(objetivo: str | None = None) -> dict:
    cfg = cargar_yaml("pipeline")
    objetivo = objetivo or cfg["modelo"]["objetivo_principal"]
    lk = lake()
    spark = crear_sesion("modelo_base")
    with registrar_corrida("modelo_base") as info:
        parts = particionar(cargar_features(spark, lk))
        for p in parts.values():
            p.cache()
        n = {k: v.count() for k, v in parts.items()}
        if n["train"] == 0 or n["valid"] == 0:
            raise RuntimeError(f"Particiones insuficientes para entrenar: {n}")
        ref = evaluar_referencia(parts, objetivo)
        modelo = entrenar_logistica(parts["train"], objetivo)
        resultados = {"objetivo": objetivo, "filas": n, "referencia": ref, "logistica": {}}
        pred_valid = con_probabilidad(modelo.transform(parts["valid"]))
        curva_valid = umb.curva(pred_valid, objetivo)
        umbrales = umb.elegir(curva_valid, cfg["modelo"]["razones_cfn_cfp"], cfg["modelo"]["costo_fp"])
        u = next(x["umbral"] for x in umbrales if x["razon_cfn_cfp"] == 5)
        for nombre in ("train", "valid", "test"):
            if n[nombre] == 0:
                continue
            pred = con_probabilidad(modelo.transform(parts[nombre]))
            resultados["logistica"][nombre] = {**aucs(pred, objetivo), **matriz(pred, objetivo, u)}
        resultados["umbrales_por_costo"] = umbrales
        lr = modelo.stages[-1]
        resultados["coeficientes_escalados_top"] = sorted(
            [(i, float(c)) for i, c in enumerate(lr.coefficients.toArray())], key=lambda t: -abs(t[1]))[:15]
        version = registrar(lk, f"logistica_{objetivo}", modelo, {**resultados, "umbrales": {"ratio5": u}}, activar=False)
        ruta = ruta_repo(cfg["evidencias"]["dir_local"], "metricas", "modelo_base.json")
        ruta.parent.mkdir(parents=True, exist_ok=True)
        ruta.write_text(json.dumps({**resultados, "version": version}, ensure_ascii=False, indent=2), encoding="utf-8")
        auc_ref = ref["valid"]["auc_roc"]
        auc_lr = resultados["logistica"]["valid"]["auc_roc"]
        _log.info(f"AUC-ROC valid: referencia {auc_ref} vs logística {auc_lr}")
        info["filas_entrada"], info["detalle"] = n["train"], f"AUC valid ref={auc_ref} lr={auc_lr}"
    return resultados


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Modelo base (referencia + logística)")
    p.add_argument("--objetivo", default=None)
    ejecutar(p.parse_args(argv).objetivo)


if __name__ == "__main__":
    main()
