"""Reglas de calidad: Q3 detecta duplicados y Q1/Q10 sobre datos reales."""

from latam_ops.transformacion.silver_vuelos import reglas_calidad


def test_reglas_ok_en_datos_reales(bronze_vra, silver_vuelos):
    reglas = {r.codigo: r for r in reglas_calidad(bronze_vra.count(), silver_vuelos)}
    assert reglas["Q1"].cumple and reglas["Q3"].cumple and reglas["Q10"].cumple


def test_q3_detecta_duplicados(bronze_vra, silver_vuelos):
    duplicado = silver_vuelos.unionByName(silver_vuelos.limit(5))
    reglas = {r.codigo: r for r in reglas_calidad(bronze_vra.count(), duplicado)}
    assert not reglas["Q3"].cumple and reglas["Q3"].valor == 5 and reglas["Q3"].critica


def test_deduplicacion_en_transformacion(bronze_vra, cfg):
    from latam_ops.comun.config import empresas_latam
    from latam_ops.transformacion.silver_vuelos import transformar_vuelos

    doble = bronze_vra.unionByName(bronze_vra)
    df = transformar_vuelos(doble, empresas_latam(), cfg["zona_horaria_vra"], -60, 1440)
    unico = transformar_vuelos(bronze_vra, empresas_latam(), cfg["zona_horaria_vra"], -60, 1440)
    assert df.count() == unico.count()
