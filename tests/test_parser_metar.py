"""Parser METAR sobre líneas reales del archivo IEM (SBAR, SBFI, SBCT, SABE, SPJC)."""

from pathlib import Path

import pytest

from latam_ops.transformacion.parser_metar import categoria_vuelo, parsear_metar

LINEAS = (Path(__file__).parent / "fixtures" / "metar_lineas_reales.txt").read_text().splitlines()


def test_niebla_y_techo_por_capa_bkn():
    r = parsear_metar(LINEAS[0])  # SBAR ... 0200 FG BKN002
    assert r["vis_m"] == 200 and r["fen_niebla"] == 1 and r["techo_ft"] == 200
    assert r["categoria_vuelo"] == "LIFR" and r["parseado_ok"]


def test_visibilidad_vertical_y_rvr_ignorado():
    r = parsear_metar(LINEAS[1])  # SBFI ... R33/0225N ... FG VV001
    assert r["techo_ft"] == 100 and r["vis_m"] == 200 and r["fen_niebla"] == 1


def test_lluvia_y_neblina():
    r = parsear_metar(LINEAS[2])  # 3000 -RA BR BKN003
    assert r["fen_lluvia"] == 1 and r["fen_neblina"] == 1 and r["techo_ft"] == 300 and r["vis_m"] == 3000
    assert r["categoria_vuelo"] == "LIFR"


def test_rafaga_y_viento_variable():
    r = parsear_metar(LINEAS[3])  # 21005G16KT 140V260
    assert (r["viento_dir"], r["viento_kt"], r["rafaga_kt"]) == (210, 5, 16)
    assert r["viento_variable"] is True and r["temp_c"] == 32 and r["rocio_c"] == 16


def test_tormenta_sin_lluvia_y_vecindad():
    r = parsear_metar(LINEAS[4])  # TS VCSH
    assert r["fen_tormenta"] == 1 and r["fen_lluvia"] == 0


def test_cavok():
    r = parsear_metar(LINEAS[5])
    assert r["cavok"] and r["vis_m"] == 10000 and r["techo_ft"] == 99999 and r["categoria_vuelo"] == "VFR"


def test_vrb():
    r = parsear_metar(LINEAS[6])
    assert r["viento_dir"] is None and r["viento_variable"] and r["viento_kt"] == 2


def test_tempo_no_contamina_condicion_actual():
    r = parsear_metar(LINEAS[7])  # condición actual 5000 -TSRA; TEMPO 1000 +TSRA
    assert r["vis_m"] == 5000 and r["tiene_tempo"] == 1 and r["tendencia"] == "TEMPO"
    assert r["fen_tormenta"] == 1 and r["fen_lluvia"] == 1


def test_rmk_ignorado_y_qnh():
    r = parsear_metar(LINEAS[9])
    assert r["qnh_hpa"] == 1013.0 and r["tendencia"] == "NOSIG" and r["fen_lluvia"] == 1


@pytest.mark.parametrize("texto", [None, "", "SBGR"])
def test_entradas_vacias_no_fallan(texto):
    assert parsear_metar(texto)["parseado_ok"] is False


def test_categoria_por_peor_condicion():
    assert categoria_vuelo(5000, 2000) == "IFR"
    assert categoria_vuelo(800, 10000) == "IFR"
    assert categoria_vuelo(None, None) is None
