"""Modelo definitivo: Random Forest y GBT para y_demora15 y y_demora60 con búsqueda de hiperparámetros
sobre la ventana TEMPORAL de validación (equivalente a TrainValidationSplit sin mezclar el tiempo),
umbral por costo y alternativa de regresión (GBTRegressor sobre minutos recortados).

Uso: python -m latam_ops.ml.modelo_final [--fraccion-train 1.0]
"""

from __future__ import annotations

import argparse
import itertools
import json

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import functions as F

from latam_ops.comun.config import cargar_yaml, lake, ruta_repo
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.ml import umbral as umb
from latam_ops.ml.datasets import cargar_features, columnas_features, particionar
from latam_ops.ml.evaluar import aucs, con_probabilidad, matriz, por_segmento
from latam_ops.ml.preparacion import etapas
from latam_ops.ml.referencia import evaluar_referencia
from latam_ops.ml.registro import registrar

_log = obtener_logger("modelo_final")
# Los árboles tratan los índices categóricos como categorías: maxBins debe superar la cardinalidad
# de destino_icao/origen_icao (~100-150 aeródromos en la red LATAM Brasil).
MAX_BINS = 256


def _grilla(d: dict) -> list[dict]:
    claves = list(d)
    return [dict(zip(claves, v, strict=True)) for v in itertools.product(*(d[k] for k in claves))]


def _clasificador(tipo: str, objetivo: str, params: dict, semilla: int):
    if tipo == "random_forest":
        return RandomForestClassifier(featuresCol="features", labelCol=objetivo, seed=semilla, subsamplingRate=0.8,
                                      maxBins=MAX_BINS, **params)
    return GBTClassifier(featuresCol="features", labelCol=objetivo, seed=semilla, maxBins=MAX_BINS, **params)


def importancias(modelo, nombres: list[str]) -> list[dict]:
    """featureImportances con nombres reales (vector sin one-hot: numéricas imputadas + índices categóricos)."""
    vec = modelo.stages[-1].featureImportances.toArray()
    pares = [{"feature": n, "importancia": round(float(v), 5)} for n, v in zip(nombres, vec, strict=False)]
    return sorted(pares, key=lambda d: -d["importancia"])


def buscar(parts, objetivo: str, cfg: dict) -> tuple:
    """Entrena cada combinación en train y elige por AUC-PR en valid (sin mirar test)."""
    semilla = cfg["modelo"]["semilla"]
    mejor, historial = None, []
    prep = Pipeline(stages=etapas(escalar=False, one_hot=False)).fit(parts["train"])
    train_p, valid_p = prep.transform(parts["train"]).cache(), prep.transform(parts["valid"]).cache()
    for tipo, grilla in (("random_forest", cfg["modelo"]["grilla_rf"]), ("gbt", cfg["modelo"]["grilla_gbt"])):
        for params in _grilla(grilla):
            clf = _clasificador(tipo, objetivo, params, semilla).fit(train_p)
            met = aucs(con_probabilidad(clf.transform(valid_p)), objetivo)
            historial.append({"tipo": tipo, "params": params, **met})
            _log.info(f"{objetivo} {tipo} {params} -> valid {met}")
            if mejor is None or met["auc_pr"] > mejor[2]["auc_pr"]:
                mejor = (tipo, params, met)
    train_p.unpersist()
    valid_p.unpersist()
    tipo, params, _ = mejor
    modelo = Pipeline(stages=etapas(escalar=False, one_hot=False) + [_clasificador(tipo, objetivo, params, semilla)]).fit(parts["train"])
    return modelo, tipo, params, historial


def alternativa_regresion(parts, cfg: dict) -> dict:
    """Alternativa descartada: GBTRegressor sobre minutos recortados a [-30, 300], comparado con predecir la media."""
    recorte = F.least(F.greatest(F.col("demora_salida_min"), F.lit(-30.0)), F.lit(300.0))
    tr = parts["train"].withColumn("minutos", recorte)
    media = tr.agg(F.avg("minutos")).first()[0]
    modelo = Pipeline(stages=etapas(escalar=False, one_hot=False) + [
        GBTRegressor(featuresCol="features", labelCol="minutos", maxIter=40, maxDepth=5, maxBins=MAX_BINS,
                     seed=cfg["modelo"]["semilla"])]).fit(tr)
    salida = {"modelo": "gbt_regresion_minutos", "media_train": round(media, 3)}
    for nombre in ("valid", "test"):
        df = parts[nombre].withColumn("minutos", recorte)
        if df.limit(1).count() == 0:
            continue
        pred = modelo.transform(df)
        triv = df.withColumn("prediction", F.lit(media))
        ev = {m: RegressionEvaluator(labelCol="minutos", predictionCol="prediction", metricName=m) for m in ("rmse", "mae", "r2")}
        salida[nombre] = {"modelo": {m: round(e.evaluate(pred), 3) for m, e in ev.items()},
                          "trivial_media": {m: round(e.evaluate(triv), 3) for m, e in ev.items()}}
    return salida


def ejecutar(fraccion_train: float = 1.0) -> dict:
    cfg = cargar_yaml("pipeline")
    lk = lake()
    spark = crear_sesion("modelo_final")
    resumen: dict = {}
    with registrar_corrida("modelo_final") as info:
        parts = particionar(cargar_features(spark, lk))
        if fraccion_train < 1.0:
            parts["train"] = parts["train"].sample(False, fraccion_train, seed=cfg["modelo"]["semilla"])
        for p in parts.values():
            p.cache()  # O3: cache antes de la búsqueda de hiperparámetros
        n = {k: v.count() for k, v in parts.items()}
        if n["train"] == 0 or n["valid"] == 0:
            raise RuntimeError(f"Particiones insuficientes: {n}")
        for objetivo in cfg["modelo"]["objetivos"]:
            if parts["train"].agg(F.sum(objetivo)).first()[0] in (None, 0):
                _log.warning(f"sin positivos de {objetivo} en train; se omite")
                continue
            modelo, tipo, params, historial = buscar(parts, objetivo, cfg)
            pv = con_probabilidad(modelo.transform(parts["valid"]))
            umbrales = umb.elegir(umb.curva(pv, objetivo), cfg["modelo"]["razones_cfn_cfp"], cfg["modelo"]["costo_fp"])
            u = next(x["umbral"] for x in umbrales if x["razon_cfn_cfp"] == 5)
            metricas = {}
            for nombre in ("valid", "test"):
                if n[nombre]:
                    pr = con_probabilidad(modelo.transform(parts[nombre]))
                    metricas[nombre] = {**aucs(pr, objetivo), **matriz(pr, objetivo, u)}
                    if nombre == "test":
                        metricas["test_por_segmento"] = por_segmento(pr, objetivo, u, cfg["modelo"]["top_aeropuertos_segmento"])
            ref = evaluar_referencia(parts, objetivo)
            meta = {"objetivo": objetivo, "tipo": tipo, "params": params, "filas": n, "busqueda": historial,
                    "umbrales_por_costo": umbrales, "umbrales": {"ratio5": u}, "metricas": metricas, "referencia": ref,
                    "importancias": importancias(modelo, columnas_features()[0] + columnas_features()[1])}
            version = registrar(lk, f"final_{objetivo}", modelo, meta)
            resumen[objetivo] = {"version": version, "tipo": tipo, "params": params,
                                 "valid": metricas.get("valid"), "test": metricas.get("test"), "referencia_valid": ref["valid"]}
        resumen["alternativa_regresion"] = alternativa_regresion(parts, cfg)
        ruta = ruta_repo(cfg["evidencias"]["dir_local"], "metricas", "modelo_final_resumen.json")
        ruta.parent.mkdir(parents=True, exist_ok=True)
        ruta.write_text(json.dumps(resumen, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        info["filas_entrada"], info["detalle"] = n["train"], json.dumps({k: v.get("tipo") for k, v in resumen.items() if isinstance(v, dict) and "tipo" in v})
    return resumen


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Modelo definitivo RF/GBT + umbral por costo")
    p.add_argument("--fraccion-train", type=float, default=1.0)
    ejecutar(p.parse_args(argv).fraccion_train)


if __name__ == "__main__":
    main()
