"""Valor de negocio: supuestos, punto de equilibrio, bootstrap por días, políticas con igual presupuesto y tabla 6.1."""

import pytest
from pyspark.sql import functions as F

from latam_ops.ml import politicas_alerta as pa
from latam_ops.ml.valor_negocio import fila_trazabilidad, hallazgos, supuestos_desde_config


def test_supuestos_resuelven_costo_y_quedan_ordenados(cfg):
    escs = supuestos_desde_config(cfg["valor_negocio"])
    assert [e.nombre for e in escs] == ["pesimista", "base", "optimista"]
    base = escs[1]
    assert base.costo_minuto_usd == pytest.approx(98.41 - 29.34)   # A4A sin combustible
    # mismo volumen de alertas y minutos: el pesimista nunca gana más que el optimista
    sumas = {"modelo_alertas": 1000.0, **{f"modelo_ah{e.minutos_por_accion}": 1000.0 * e.minutos_por_accion for e in escs}}
    netos = [pa.beneficio_politica(sumas, "modelo", e) for e in escs]
    assert netos == sorted(netos)


def test_beneficio_y_equilibrio():
    assert pa.beneficio(1000, 10, 50.0, 150.0) == pytest.approx(48500.0)
    # f* = a·N / (c·minutos) = 150·10 / (50·1000)
    assert pa.fraccion_equilibrio(1000, 10, 50.0, 150.0) == pytest.approx(0.03)
    assert pa.fraccion_equilibrio(0, 10, 50.0, 150.0) is None


def test_minutos_equilibrio_interpola_y_devuelve_none_si_no_alcanza():
    # 100 alertas, US$ 50/min, US$ 150/acción -> hacen falta 300 minutos ahorrados
    curva = {5: 200.0, 10: 400.0, 20: 600.0}
    assert pa.minutos_equilibrio(curva, 100, 50.0, 150.0) == pytest.approx(7.5)
    assert pa.minutos_equilibrio(curva, 100, 50.0, 1500.0) is None


def test_bootstrap_pareado_y_reproducible():
    dias = [{"fecha": i, "vuelos": 10.0, "positivos": 2.0, "x": float(i % 3)} for i in range(30)]

    def est(s):
        return {"media_x": s["x"] / s["dias"], "positivos": s["positivos"]}

    a = pa.bootstrap(dias, est, repeticiones=200, semilla=7)
    b = pa.bootstrap(dias, est, repeticiones=200, semilla=7)
    assert a == b
    assert a["media_x"]["p_inf"] <= a["media_x"]["puntual"] <= a["media_x"]["p_sup"]
    # todos los días tienen 2 positivos: el intervalo no puede tener ancho
    assert a["positivos"]["p_inf"] == a["positivos"]["p_sup"] == 60.0


def test_serie_diaria_suma_grupos_y_quita_prefijo():
    filas = [
        {"fecha_local": "d1", "grupo": "SBGR", "vuelos": 10, "pos_ambar": 3, "pos_rojo": 1, "min_tope_total": 50.0,
         "pax_total": 0.0, "ambar_tope5_modelo_alertas": 5.0, "ambar_tope5_modelo_vp": 2.0},
        {"fecha_local": "d1", "grupo": "resto", "vuelos": 4, "pos_ambar": 1, "pos_rojo": 0, "min_tope_total": 5.0,
         "pax_total": 0.0, "ambar_tope5_modelo_alertas": 4.0, "ambar_tope5_modelo_vp": 1.0},
    ]
    red = pa.serie_diaria(filas, "ambar", "tope5", pa.AMBITOS["red"])
    assert red == [{"vuelos": 14.0, "positivos": 4.0, "pos15": 4.0, "pos60": 1.0, "min_tope_total": 55.0,
                    "pax_total": 0.0, "modelo_alertas": 9.0, "modelo_vp": 3.0, "fecha": "d1"}]
    assert pa.serie_diaria(filas, "ambar", "tope5", ("SBSP",)) == []


def test_politicas_mismo_presupuesto_con_vuelos_reales(silver_vuelos, clima_horario, cfg):
    """Con un puntaje que conoce la etiqueta, el modelo nunca acierta menos que la referencia con igual presupuesto."""
    from latam_ops.transformacion.gold_features import construir_features

    f = (construir_features(silver_vuelos, clima_horario, cfg)
         .withColumn("prob_demora15", F.col("y_demora15").cast("double"))
         .withColumn("prob_demora60", F.col("y_demora60").cast("double")))
    d = pa.preparar(f, 0.2, cfg["zona_horaria_vra"], ["SBGR", "SBSP"], 120, [10])
    d, claves = pa.banderas(d, {"ambar": 0.5, "rojo": 0.5}, [3])
    filas = pa.agregar(d, claves, [5, 10], [10], con_pasajeros=False)
    for regla in ("umbral", "tope3", "cuota3"):
        s = pa.sumar(pa.serie_diaria(filas, "ambar", regla, pa.AMBITOS["red"]))
        assert s["modelo_alertas"] == s["referencia_alertas"] == s["aleatoria_alertas"]
        assert s["modelo_vp"] >= s["referencia_vp"]
        assert s["modelo_vp"] >= s["aleatoria_vp"] - 1e-9
        assert s["modelo_ah5"] <= s["modelo_ah10"] <= s["modelo_min_tope"] + 1e-9


def test_trazabilidad_rechaza_filas_incompletas():
    fila = fila_trazabilidad("o", "h", "d", "r", "e")
    assert set(fila) == {"objetivo", "hallazgo", "decision", "responsable", "efecto"}
    with pytest.raises(ValueError):
        fila_trazabilidad("o", "h", "d", " ", "e")


def test_hallazgos_ordena_por_riesgo(spark):
    filas = []
    for i in range(600):
        filas.append(("SBGR", 8, "VFR", 0, 0, 0, 0, 0.05, 1 if i % 10 == 0 else 0))
        filas.append(("SBSP", 19, "IFR", 1, 0, 0, 0, 0.6, 1 if i % 2 == 0 else 0))
    df = spark.createDataFrame(filas, "origen_icao string, hora_local int, o_categoria_vuelo string, "
                                      "o_fen_niebla int, o_fen_neblina int, o_fen_lluvia int, o_fen_tormenta int, "
                                      "red_frac_atrasadas double, y_demora15 int")
    h = hallazgos(df)
    assert h["aeropuertos"][0]["grupo"] == "SBSP" and h["aeropuertos"][0]["tasa_demora15"] == 0.5
    assert h["franjas"][0]["grupo"] == "18-23"
    assert h["clima"][0]["grupo"] == "niebla"
    assert h["red"][0]["grupo"] == ">=50 %"
