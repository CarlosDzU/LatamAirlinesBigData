"""Valor de negocio: cuentas de ahorro, vuelos recuperados, escenarios y tabla de trazabilidad."""

import pytest

from latam_ops.ml.valor_negocio import (
    Escenario,
    escenarios_desde_config,
    fila_trazabilidad,
    hallazgos,
    recuperados,
    tasa_tras_mitigar,
    valor_escenario,
)


def test_valor_escenario_cuentas():
    esc = Escenario("base", fraccion_mitigable=0.2, costo_accion_usd=150)
    v = valor_escenario(vp=100, fp=300, minutos_vp=5000, costo_minuto=98.41, esc=esc)
    assert v["minutos_evitados"] == 1000.0
    assert v["ahorro_usd"] == pytest.approx(98410.0)
    assert v["costo_alertas_usd"] == 60000.0
    assert v["beneficio_neto_usd"] == pytest.approx(38410.0)
    assert v["roi"] == pytest.approx(0.640, abs=1e-3)


def test_recuperados_solo_los_que_bajan_del_umbral():
    # con 20 % menos: 16 -> 12.8 (sí), 18 -> 14.4 (sí), 19 -> 15.2 (no), 60 -> 48 (no); 10 no estaba demorado
    assert recuperados([10, 16, 18, 19, 60], 0.20) == 2
    assert recuperados([16, 18], 0.0) == 0


def test_tasa_tras_mitigar():
    assert tasa_tras_mitigar(0.20, 1000, 50) == pytest.approx(0.15)
    assert tasa_tras_mitigar(0.01, 100, 50) == 0.0
    assert tasa_tras_mitigar(0.20, 0, 5) == 0.20


def test_escenarios_ordenados_por_beneficio(cfg):
    escs = escenarios_desde_config(cfg["valor_negocio"])
    assert [e.nombre for e in escs] == ["pesimista", "base", "optimista"]
    netos = [valor_escenario(500, 1500, 30000, cfg["valor_negocio"]["costo_minuto_usd"], e)["beneficio_neto_usd"]
             for e in escs]
    assert netos == sorted(netos)


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
