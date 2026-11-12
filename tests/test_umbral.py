"""Umbral por costo: tabla de sensibilidad y umbral teórico C_FP/(C_FP+C_FN)."""

from latam_ops.ml.umbral import curva, elegir


def test_curva_y_eleccion(spark):
    datos = [(0.05, 0), (0.10, 0), (0.20, 0), (0.30, 1), (0.40, 0), (0.55, 1), (0.70, 1), (0.90, 1)]
    df = spark.createDataFrame(datos, "prob double, y int")
    c = curva(df, "y")
    assert c[0]["vp"] == 4 and c[0]["fp"] == 4
    assert c[50] == {"umbral": 0.5, "vp": 3, "fp": 0, "fn": 1, "vn": 4}
    sel = {x["razon_cfn_cfp"]: x for x in elegir(c, [1, 10])}
    assert sel[10]["umbral"] <= sel[1]["umbral"]  # FN más caro -> umbral más bajo
    assert sel[10]["umbral_teorico"] == round(1 / 11, 4)
