"""Contratos YAML -> StructType y proyección estricta."""

from pathlib import Path

import pytest

from latam_ops.comun.config import ruta_repo
from latam_ops.comun.contratos import ErrorContrato, aplicar_contrato, esquema

CONTRATOS = sorted((Path(ruta_repo("contratos"))).rglob("*.yaml"))


@pytest.mark.parametrize("archivo", CONTRATOS, ids=lambda p: f"{p.parent.name}/{p.stem}")
def test_contrato_genera_structtype(archivo):
    s = esquema(archivo.parent.name, archivo.stem)
    assert len(s.fields) > 0 and len({f.name for f in s.fields}) == len(s.fields)


def test_silver_cumple_contrato(silver_vuelos):
    assert silver_vuelos.columns == [f.name for f in esquema("silver", "vuelos").fields]


def test_falta_columna_falla(silver_vuelos):
    with pytest.raises(ErrorContrato):
        aplicar_contrato(silver_vuelos.drop("id_vuelo"), "silver", "vuelos")


def test_features_cumplen_contrato(silver_vuelos, clima_horario, cfg):
    from latam_ops.transformacion.gold_features import construir_features

    f = aplicar_contrato(construir_features(silver_vuelos, clima_horario, cfg), "gold", "features_salida")
    assert f.count() > 0
