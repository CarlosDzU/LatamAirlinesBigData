"""Valor de negocio para LATAM: línea base, efecto de la alerta en minutos y US$, sensibilidad y trazabilidad.

Toma las predicciones del test (2025) al umbral elegido en validación y las convierte en:
- vuelos alertados a tiempo (VP), falsas alarmas (FP) y demoras no avisadas (FN);
- minutos de demora evitables y su costo, con los supuestos de config/pipeline.yaml (valor_negocio);
- cambio estimado en la tasa de salidas con 15 min o más frente a la línea base de 2025.

También calcula hallazgos accionables (aeropuertos, franjas, clima y estado de la red) y arma la tabla
de trazabilidad objetivo -> hallazgo -> decisión -> responsable -> efecto.

Uso: python -m latam_ops.ml.valor_negocio [--split test]
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from latam_ops.comun.almacen import Almacen
from latam_ops.comun.config import Lake, cargar_yaml, lake, ruta_repo
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.ml.datasets import cargar_features
from latam_ops.ml.umbral import curva, elegir

_log = obtener_logger("valor_negocio")
UMBRAL_DEMORA = 15


# --------------------------------------------------------------------------- cálculo puro (sin Spark)

@dataclass(frozen=True)
class Escenario:
    """Supuestos de un escenario de valor."""

    nombre: str
    fraccion_mitigable: float   # parte de la demora que se evita cuando el CCO actúa a T-2 h
    costo_accion_usd: float     # costo de revisar/actuar sobre un vuelo alertado (sea VP o FP)


def escenarios_desde_config(cfg: dict) -> list[Escenario]:
    """Lee los escenarios de valor_negocio.escenarios en el orden pesimista, base, optimista."""
    return [Escenario(n, float(e["fraccion_mitigable"]), float(e["costo_accion_usd"]))
            for n, e in cfg["escenarios"].items()]


def valor_escenario(vp: int, fp: int, minutos_vp: float, costo_minuto: float, esc: Escenario) -> dict:
    """Beneficio de actuar sobre las alertas.

    minutos_vp: suma de minutos de demora de los vuelos alertados correctamente.
    ahorro = minutos_vp * fraccion_mitigable * costo_minuto; costo = (vp + fp) * costo_accion.
    """
    minutos_evitados = minutos_vp * esc.fraccion_mitigable
    ahorro = minutos_evitados * costo_minuto
    costo = (vp + fp) * esc.costo_accion_usd
    return {"escenario": esc.nombre, "fraccion_mitigable": esc.fraccion_mitigable,
            "costo_accion_usd": esc.costo_accion_usd, "alertas": vp + fp, "vp": vp, "fp": fp,
            "minutos_evitados": round(minutos_evitados, 1), "ahorro_usd": round(ahorro, 2),
            "costo_alertas_usd": round(costo, 2), "beneficio_neto_usd": round(ahorro - costo, 2),
            "roi": round((ahorro - costo) / costo, 3) if costo else None}


def recuperados(demoras_vp: list[float], fraccion: float, umbral: int = UMBRAL_DEMORA) -> int:
    """Vuelos alertados que, con la demora reducida en `fraccion`, quedan por debajo del umbral."""
    return sum(1 for d in demoras_vp if d >= umbral and d * (1 - fraccion) < umbral)


def tasa_tras_mitigar(tasa_base: float, total: int, recuperados_n: int) -> float:
    """Nueva tasa de salidas >= umbral si los vuelos recuperados dejan de contar como demorados."""
    if total <= 0:
        return tasa_base
    return max(tasa_base - recuperados_n / total, 0.0)


def fila_trazabilidad(objetivo: str, hallazgo: str, decision: str, responsable: str, efecto: str) -> dict:
    """Una fila de la tabla 6.1; si falta alguna columna la recomendación no está lista."""
    fila = {"objetivo": objetivo, "hallazgo": hallazgo, "decision": decision, "responsable": responsable,
            "efecto": efecto}
    vacias = [k for k, v in fila.items() if not str(v).strip()]
    if vacias:
        raise ValueError(f"Fila de trazabilidad incompleta, faltan: {vacias}")
    return fila


# --------------------------------------------------------------------------- Spark

def linea_base(spark: SparkSession, lk: Lake, anio: int | None = None) -> dict:
    """Tasa de salidas LATAM con >= 15 y >= 60 min y de cancelación, del último año completo en silver."""
    v = spark.read.parquet(lk.ruta("silver_vuelos")).where(F.col("es_latam"))
    anios = sorted(r["year"] for r in v.select("year").distinct().collect())
    if not anios:
        raise RuntimeError("silver/vuelos está vacío")
    anio = anio if anio in anios else (2025 if 2025 in anios else anios[-1])
    d = v.where(F.col("year") == anio)
    tot = d.count()
    oper = d.where(~F.col("cancelado") & ~F.col("es_atipico") & F.col("demora_salida_min").isNotNull())
    r = oper.agg(F.count("*").alias("n"), F.avg((F.col("demora_salida_min") >= 15).cast("double")).alias("t15"),
                 F.avg((F.col("demora_salida_min") >= 60).cast("double")).alias("t60"),
                 F.avg("demora_salida_min").alias("media")).first()
    canc = d.where(F.col("cancelado")).count()
    cfg = cargar_yaml("pipeline")["valor_negocio"]["linea_base"]
    return {"anio": anio, "vuelos_programados": tot, "vuelos_operados": int(r["n"]),
            "tasa_salida_15": round(float(r["t15"] or 0), 4), "tasa_salida_60": round(float(r["t60"] or 0), 4),
            "demora_media_min": round(float(r["media"] or 0), 2), "tasa_cancelacion": round(canc / max(tot, 1), 4),
            "otp_llegada_cirium": cfg["otp_cirium_2025"], "fuente_otp": cfg["fuente_otp"]}


def _datos_evaluacion(spark: SparkSession, lk: Lake) -> DataFrame:
    """Features con etiquetas + predicciones de valid y test."""
    try:
        pred = spark.read.parquet(lk.ruta("gold_predicciones")).select("id_vuelo", "prob_demora15", "prob_demora60")
    except AnalysisException as exc:
        raise RuntimeError("No hay gold/predicciones: primero hay que entrenar y predecir") from exc
    cols = ["id_vuelo", "split", "origen_icao", "destino_icao", "hora_local", "o_categoria_vuelo", "o_fen_niebla",
            "o_fen_neblina", "o_fen_lluvia", "o_fen_tormenta", "red_frac_atrasadas", "y_demora15", "y_demora60",
            "demora_salida_min", "year", "month"]
    feats = cargar_features(spark, lk)
    return feats.select(*[c for c in cols if c in feats.columns]).join(pred, "id_vuelo")


def _evaluar_nivel(df_eval: DataFrame, df_valid: DataFrame, objetivo: str, col_prob: str, razones: list[float],
                   razon_principal: float, costo_minuto: float, escenarios: list[Escenario], total_operados: int,
                   tasa_base: float, umbral_dem: int) -> tuple[list[dict], list[dict], dict]:
    """Elige umbrales en validación y mide el efecto en el split de evaluación."""
    c_valid = curva(df_valid.withColumnRenamed(col_prob, "prob"), objetivo)
    umbrales = elegir(c_valid, razones)
    sensibilidad, valor = [], []
    principal = {}
    for u in umbrales:
        pred = df_eval.withColumn("alerta", (F.col(col_prob) >= u["umbral"]).cast("int"))
        y = F.col(objetivo).cast("int")
        m = pred.agg(
            F.sum(((F.col("alerta") == 1) & (y == 1)).cast("long")).alias("vp"),
            F.sum(((F.col("alerta") == 1) & (y == 0)).cast("long")).alias("fp"),
            F.sum(((F.col("alerta") == 0) & (y == 1)).cast("long")).alias("fn"),
            F.sum(((F.col("alerta") == 0) & (y == 0)).cast("long")).alias("vn"),
            F.sum(F.when((F.col("alerta") == 1) & (y == 1), F.col("demora_salida_min"))).alias("min_vp"),
            F.sum(F.when((F.col("alerta") == 0) & (y == 1), F.col("demora_salida_min"))).alias("min_fn"),
        ).first()
        vp, fp, fn, vn = (int(m[k] or 0) for k in ("vp", "fp", "fn", "vn"))
        min_vp = float(m["min_vp"] or 0)
        fila = {"objetivo": objetivo, "razon_cfn_cfp": u["razon_cfn_cfp"], "umbral": u["umbral"], "vp": vp, "fp": fp,
                "fn": fn, "vn": vn, "precision": round(vp / max(vp + fp, 1), 4), "recall": round(vp / max(vp + fn, 1), 4),
                "alertas_por_dia": None, "minutos_demora_vp": round(min_vp, 1),
                "minutos_demora_fn": round(float(m["min_fn"] or 0), 1)}
        dias = df_eval.select(F.to_date(F.make_date("year", "month", F.lit(1))).alias("m")).distinct().count() * 30.4
        fila["alertas_por_dia"] = round((vp + fp) / max(dias, 1), 1)
        sensibilidad.append(fila)
        if u["razon_cfn_cfp"] == razon_principal:
            principal = fila
            vp_df = pred.where((F.col("alerta") == 1) & (F.col(objetivo) == 1))
            for esc in escenarios:
                rec = vp_df.where((F.col("demora_salida_min") >= umbral_dem)
                                  & (F.col("demora_salida_min") * (1 - esc.fraccion_mitigable) < umbral_dem)).count()
                v = valor_escenario(vp, fp, min_vp, costo_minuto, esc)
                nueva = tasa_tras_mitigar(tasa_base, total_operados, rec)
                v.update({"objetivo": objetivo, "umbral": u["umbral"], "vuelos_recuperados": rec,
                          "tasa_base": round(tasa_base, 4), "tasa_tras_mitigar": round(nueva, 4),
                          "mejora_puntos_porcentuales": round((tasa_base - nueva) * 100, 2)})
                valor.append(v)
    return sensibilidad, valor, principal


def _tasa_por(df: DataFrame, col: str, objetivo: str = "y_demora15", minimo: int = 500) -> list[dict]:
    g = (df.groupBy(col).agg(F.count("*").alias("vuelos"), F.avg(F.col(objetivo).cast("double")).alias("tasa"))
         .where(F.col("vuelos") >= minimo).orderBy(F.desc("tasa")))
    return [{"grupo": r[col], "vuelos": int(r["vuelos"]), "tasa_demora15": round(float(r["tasa"]), 4)} for r in g.collect()]


def hallazgos(df: DataFrame) -> dict[str, list[dict]]:
    """Dónde y cuándo se concentra el riesgo (sobre el split evaluado)."""
    franja = (F.when(F.col("hora_local") < 6, "00-05").when(F.col("hora_local") < 12, "06-11")
              .when(F.col("hora_local") < 18, "12-17").otherwise("18-23"))
    clima = (F.when(F.col("o_fen_tormenta") == 1, "tormenta").when(F.col("o_fen_niebla") == 1, "niebla")
             .when(F.col("o_categoria_vuelo").isin("IFR", "LIFR"), "IFR/LIFR sin niebla")
             .when(F.col("o_fen_lluvia") == 1, "lluvia").when(F.col("o_categoria_vuelo").isNull(), "sin METAR")
             .otherwise("VFR/MVFR sin fenómenos"))
    red = (F.when(F.col("red_frac_atrasadas").isNull(), "sin salidas previas")
           .when(F.col("red_frac_atrasadas") < 0.1, "<10 %").when(F.col("red_frac_atrasadas") < 0.3, "10-30 %")
           .when(F.col("red_frac_atrasadas") < 0.5, "30-50 %").otherwise(">=50 %"))
    top = [r["origen_icao"] for r in df.groupBy("origen_icao").count().orderBy(F.desc("count")).limit(15).collect()]
    return {
        "aeropuertos": _tasa_por(df.where(F.col("origen_icao").isin(top)), "origen_icao"),
        "franjas": _tasa_por(df.withColumn("franja", franja), "franja"),
        "clima": _tasa_por(df.withColumn("clima_origen", clima), "clima_origen", minimo=100),
        "red": _tasa_por(df.withColumn("red_origen_t2h", red), "red_origen_t2h", minimo=100),
    }


def trazabilidad(base: dict, principal: dict, valor_base: dict, hall: dict, total_eval: int, tasa_eval: float,
                 periodo: str) -> list[dict]:
    """Tabla 6.1 con cifras del pipeline."""
    filas = []
    t0 = base["tasa_salida_15"] * 100
    filas.append(fila_trazabilidad(
        "Avisar a T-2 h los vuelos con riesgo de salir con 15 min o más",
        f"Al umbral elegido (C_FN/C_FP = {principal.get('razon_cfn_cfp')}) el modelo detecta "
        f"{principal.get('recall', 0) * 100:.1f} % de las demoras con precisión {principal.get('precision', 0) * 100:.1f} % "
        f"({principal.get('alertas_por_dia')} alertas/día) en {periodo}",
        "Usar la lista ámbar como cola priorizada de revisión del turno en SBGR y SBSP: avión o tripulación de reserva,"
        " re-protección de conexiones y aviso temprano a pasajeros",
        "Jefe de turno del CCO de LATAM Brasil",
        f"Escenario base: {valor_base.get('vuelos_recuperados', 0)} vuelos dejan de salir con 15 min o más; la tasa pasa de "
        f"{t0:.1f} % a {valor_base.get('tasa_tras_mitigar', 0) * 100:.1f} %; beneficio neto "
        f"US$ {valor_base.get('beneficio_neto_usd', 0):,.0f}"))
    if hall["aeropuertos"]:
        a = hall["aeropuertos"][0]
        exceso = int(a["vuelos"] * max(a["tasa_demora15"] - tasa_eval, 0))
        filas.append(fila_trazabilidad(
            "Reducir la tasa de salidas con 15 min o más en los aeropuertos críticos",
            f"{a['grupo']} tiene {a['tasa_demora15'] * 100:.1f} % de salidas con 15 min o más frente a "
            f"{tasa_eval * 100:.1f} % de la red ({a['vuelos']} vuelos)",
            f"Revisar holguras de rotación y recursos de rampa en {a['grupo']} en las franjas de mayor riesgo",
            "Gerencia de operaciones aeroportuarias del hub",
            f"Hasta {exceso} vuelos menos con 15 min o más si {a['grupo']} se lleva al promedio de la red"))
    if hall["franjas"]:
        f = hall["franjas"][0]
        exceso = int(f["vuelos"] * max(f["tasa_demora15"] - tasa_eval, 0))
        filas.append(fila_trazabilidad(
            "Ajustar la programación donde la demora se acumula",
            f"La franja {f['grupo']} h concentra {f['tasa_demora15'] * 100:.1f} % de salidas con 15 min o más",
            f"Aumentar la holgura de giro en rotaciones que salen en la franja {f['grupo']} h",
            "Planeamiento de red y programación",
            f"Hasta {exceso} vuelos menos con 15 min o más si la franja baja al promedio"))
    clima = [c for c in hall["clima"] if c["grupo"] not in ("VFR/MVFR sin fenómenos", "sin METAR")]
    vfr = next((c for c in hall["clima"] if c["grupo"] == "VFR/MVFR sin fenómenos"), None)
    if clima and vfr:
        c = clima[0]
        filas.append(fila_trazabilidad(
            "Anticipar el efecto del clima reportado en el METAR",
            f"Con {c['grupo']} en el origen a T-2 h la tasa es {c['tasa_demora15'] * 100:.1f} % frente a "
            f"{vfr['tasa_demora15'] * 100:.1f} % con tiempo VFR ({c['vuelos']} vuelos)",
            f"Protocolo de clima: si el METAR de T-2 h reporta {c['grupo']}, pre-asignar recursos y avisar a pasajeros",
            "Despacho de vuelos y meteorología del CCO",
            f"Prioriza {c['vuelos']} salidas del periodo con riesgo {c['tasa_demora15'] / max(vfr['tasa_demora15'], 1e-9):.1f}"
            " veces mayor"))
    if hall["red"]:
        r = hall["red"][0]
        filas.append(fila_trazabilidad(
            "Cortar la propagación de demoras en la red",
            f"Cuando {r['grupo']} de las salidas previas del origen ya iban atrasadas, la tasa sube a "
            f"{r['tasa_demora15'] * 100:.1f} %",
            "Alerta de red: si en [T-4 h, T-2 h) la fracción de salidas atrasadas supera 30 %, escalar al coordinador del hub",
            "Coordinador de hub del CCO",
            f"Foco en {r['vuelos']} salidas del periodo con el mayor riesgo observado"))
    filas.append(fila_trazabilidad(
        "Mantener la calidad de la alerta en el tiempo",
        "Las métricas cambian por temporada y la red cambia (deriva medida con PSI)",
        "Revisar cada mes el KPI K4 (precisión y recall de la alerta) y reentrenar si el recall baja de 0,60",
        "Analítica de operaciones",
        f"Sostener el efecto estimado sobre la línea base de {t0:.1f} % de salidas con 15 min o más"))
    return filas


def _escribir_csv(directorio: Path, nombre: str, filas: list[dict]) -> None:
    if not filas:
        return
    directorio.mkdir(parents=True, exist_ok=True)
    with open(directorio / nombre, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)


def ejecutar(split: str = "test") -> dict:
    """Calcula y publica el valor de negocio (CSV en docs/evidencias/valor y JSON en gold/metricas)."""
    cfg_total = cargar_yaml("pipeline")
    cfg = cfg_total["valor_negocio"]
    lk = lake()
    spark = crear_sesion("valor_negocio")
    with registrar_corrida("valor_negocio") as info:
        base = linea_base(spark, lk)
        datos = _datos_evaluacion(spark, lk).cache()
        splits = {r["split"] for r in datos.select("split").distinct().collect()}
        if split not in splits:
            alterno = "valid" if "valid" in splits else None
            if not alterno:
                raise RuntimeError(f"No hay predicciones de {split} ni de valid para medir el valor")
            _log.warning(f"sin predicciones de {split}; el valor se mide sobre {alterno}")
            split = alterno
        df_eval = datos.where(F.col("split") == split)
        df_valid = datos.where(F.col("split") == "valid")
        total = df_eval.count()
        tasa_eval = float(df_eval.agg(F.avg(F.col("y_demora15").cast("double"))).first()[0] or 0)
        meses = sorted({f"{r['year']}-{r['month']:02d}" for r in df_eval.select("year", "month").distinct().collect()})
        periodo = f"{split} {meses[0]}..{meses[-1]}"
        escenarios = escenarios_desde_config(cfg)
        razones = [float(x) for x in cfg_total["modelo"]["razones_cfn_cfp"]]
        sens, valor, principal = [], [], {}
        for objetivo, col, umbral_dem, tasa in (("y_demora15", "prob_demora15", 15, tasa_eval),
                                                ("y_demora60", "prob_demora60", 60, None)):
            if tasa is None:
                tasa = float(df_eval.agg(F.avg(F.col(objetivo).cast("double"))).first()[0] or 0)
            s, v, p = _evaluar_nivel(df_eval, df_valid, objetivo, col, razones, float(cfg["razon_principal"]),
                                     float(cfg["costo_minuto_usd"]), escenarios, total, tasa, umbral_dem)
            sens += s
            valor += v
            if objetivo == "y_demora15":
                principal = p
        hall = hallazgos(df_eval)
        v_base = next((v for v in valor if v["objetivo"] == "y_demora15" and v["escenario"] == "base"), {})
        traz = trazabilidad(base, principal, v_base, hall, total, tasa_eval, periodo)
        resumen = {"periodo_evaluado": periodo, "vuelos_evaluados": total, "tasa_demora15_evaluada": round(tasa_eval, 4),
                   "linea_base": base, "supuestos": {k: cfg[k] for k in ("costo_minuto_usd", "fuente_costo_minuto",
                                                                           "escenarios", "nota_supuestos")},
                   "umbral_principal": principal, "valor": valor, "trazabilidad": traz, "hallazgos": hall}
        salida = ruta_repo(cfg_total["evidencias"]["dir_local"], "valor")
        _escribir_csv(salida, "linea_base.csv", [base])
        _escribir_csv(salida, "valor_escenarios.csv", valor)
        _escribir_csv(salida, "sensibilidad_umbral.csv", sens)
        _escribir_csv(salida, "trazabilidad.csv", traz)
        for k, filas in hall.items():
            _escribir_csv(salida, f"hallazgos_{k}.csv", filas)
        Almacen(lk).escribir_json(f"{lk.relativa('gold_metricas')}/valor_negocio.json", resumen)
        datos.unpersist()
        info["filas_entrada"] = total
        info["detalle"] = f"beneficio neto base US$ {v_base.get('beneficio_neto_usd')}"
        _log.info(f"valor de negocio ({periodo}): {info['detalle']}")
        return resumen


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Valor de negocio de la alerta")
    p.add_argument("--split", default="test")
    ejecutar(p.parse_args(argv).split)


if __name__ == "__main__":
    main()
