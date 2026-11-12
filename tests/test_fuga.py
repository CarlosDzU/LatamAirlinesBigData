"""Anti-fuga: ninguna feature del modelo describe el resultado del propio vuelo."""

import yaml

from latam_ops.comun.config import ruta_repo
from latam_ops.ml.datasets import columnas_features
from latam_ops.transformacion.gold_features import COLUMNAS_PROHIBIDAS


def test_features_no_intersectan_prohibidas():
    num, cat = columnas_features()
    assert not (set(num) | set(cat)) & COLUMNAS_PROHIBIDAS


def test_etiquetas_no_son_features():
    num, cat = columnas_features()
    contrato = yaml.safe_load(open(ruta_repo("contratos", "gold", "features_salida.yaml"), encoding="utf-8"))
    assert not set(contrato["etiquetas"]) & (set(num) | set(cat))


def test_vector_de_features_sin_columnas_prohibidas(spark):
    from latam_ops.ml.preparacion import etapas

    entradas = set()
    for e in etapas():
        for metodo in ("getInputCols", "getInputCol"):
            if hasattr(e, metodo):
                try:
                    v = getattr(e, metodo)()
                    entradas |= set(v if isinstance(v, list) else [v])
                except Exception:
                    pass
    base = {c.removesuffix("_imp").removesuffix("_idx").removesuffix("_ohe") for c in entradas}
    assert not base & COLUMNAS_PROHIBIDAS
