"""K3: fracción de salidas ya atrasadas en el aeródromo en [T-4 h, T-2 h), llevada a fact_vuelo desde las features."""

from pyspark.sql import functions as F

from latam_ops.comun.contratos import aplicar_contrato
from latam_ops.transformacion.gold_dimensional import construir_fact
from latam_ops.transformacion.gold_features import construir_features
from latam_ops.transformacion.gold_kpi import kpi_diario


def test_k3_es_la_media_de_red_frac_atrasadas(silver_vuelos, clima_horario, cfg):
    feats = construir_features(silver_vuelos, clima_horario, cfg)
    fact = aplicar_contrato(construir_fact(silver_vuelos, feats, None), "gold", "fact_vuelo")
    assert "red_frac_atrasadas" in fact.columns
    con_dato = fact.where(F.col("red_frac_atrasadas").isNotNull())
    assert con_dato.count() > 0
    assert con_dato.where((F.col("red_frac_atrasadas") < 0) | (F.col("red_frac_atrasadas") > 1)).count() == 0

    k = kpi_diario(fact).where(F.col("origen_icao") == "SBGR").orderBy("fecha").first()
    esperado = (fact.where((F.col("origen_key") == "SBGR")
                           & (F.to_date(F.col("fecha_key").cast("string"), "yyyyMMdd") == F.lit(k["fecha"])))
                .agg(F.avg("red_frac_atrasadas")).first()[0])
    assert k["k3_indice_propagacion"] == esperado
    # ya no se satura: no puede valer 1 en todos los días del hub
    valores = [r["k3_indice_propagacion"] for r in kpi_diario(fact).where(F.col("origen_icao") == "SBGR").collect()]
    assert any(v is not None and v < 0.9 for v in valores)
