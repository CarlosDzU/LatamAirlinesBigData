"""Etapas de preparación de features de Spark ML (se ajustan solo con train dentro del Pipeline)."""

from __future__ import annotations

from pyspark.ml.feature import Imputer, OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler

from latam_ops.ml.datasets import columnas_features


def etapas(escalar: bool = True, one_hot: bool = True) -> list:
    """StringIndexer(+OneHot) para categóricas, Imputer (mediana) para numéricas, VectorAssembler y StandardScaler."""
    num, cat = columnas_features()
    idx = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep", stringOrderType="frequencyDesc")
           for c in cat]
    salida_cat = [f"{c}_idx" for c in cat]
    pasos: list = list(idx)
    if one_hot:
        pasos.append(OneHotEncoder(inputCols=salida_cat, outputCols=[f"{c}_ohe" for c in cat], handleInvalid="keep"))
        salida_cat = [f"{c}_ohe" for c in cat]
    pasos.append(Imputer(inputCols=num, outputCols=[f"{c}_imp" for c in num], strategy="median"))
    pasos.append(VectorAssembler(inputCols=[f"{c}_imp" for c in num] + salida_cat, outputCol="features_sin_escala",
                                 handleInvalid="keep"))
    if escalar:
        pasos.append(StandardScaler(inputCol="features_sin_escala", outputCol="features", withMean=False, withStd=True))
    else:
        pasos[-1].setOutputCol("features")
    return pasos


def nombres_vector() -> list[str]:
    """Nombres de las columnas de entrada del vector (las numéricas primero, luego categóricas)."""
    num, cat = columnas_features()
    return num + cat
