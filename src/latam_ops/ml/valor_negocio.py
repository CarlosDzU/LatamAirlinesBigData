"""Valor de negocio para LATAM: línea base, valor incremental del modelo, capacidad del CCO y punto de equilibrio.

Toma las predicciones del test (2025) y responde, en este orden:
1. ¿Cuánto mejor que una tabla de tasas por aeropuerto y hora es el modelo, con el mismo número de vuelos
   revisados? (valor_incremental.csv)
2. Si el jefe de turno solo puede actuar sobre K vuelos por aeropuerto y día, ¿cuántas demoras captura y cuánto
   vale? (capacidad_topk.csv)
3. ¿Cuántos minutos tiene que ahorrar cada acción para que la alerta se pague, según el costo del minuto y de
   la acción? (punto_equilibrio.csv) Es la forma honesta de presentar supuestos sin fuente.
4. ¿Qué tan estables son esas cifras? Intervalos por bootstrap de días (intervalos_bootstrap.csv).
Además: escenarios (valor_escenarios.csv), sensibilidad del umbral, hallazgos y la tabla de trazabilidad 6.1.

Solo entran las empresas de pasajeros (config/aerolineas_latam.yaml, tipos_incluidos_modelo).

Uso: python -m latam_ops.ml.valor_negocio [--split test]
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from latam_ops.comun.almacen import Almacen
from latam_ops.comun.config import Lake, cargar_yaml, empresas_modelo, lake, ruta_repo
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.log import obtener_logger
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.ml import politicas_alerta as pa
from latam_ops.ml.datasets import cargar_features
from latam_ops.ml.umbral import curva, elegir

_log = obtener_logger("valor_negocio")
TECHO_REACCIONARIA = 0.46  # CODA 2024: parte de los minutos de demora heredada del vuelo anterior


# --------------------------------------------------------------------------- supuestos y tabla 6.1

def supuestos_desde_config(cfg: dict) -> list[pa.Supuestos]:
    """Escenarios pesimista, base y optimista con su costo por minuto resuelto desde costos_minuto."""
    costos = cfg["costos_minuto"]
    return [pa.Supuestos(nombre, int(e["minutos_por_accion"]), float(costos[e["costo_minuto"]]["usd"]),
                         float(e["costo_accion_usd"])) for nombre, e in cfg["escenarios"].items()]


def fila_trazabilidad(objetivo: str, hallazgo: str, decision: str, responsable: str, efecto: str) -> dict:
    """Una fila de la tabla 6.1; si falta alguna columna la recomendación no está lista."""
    fila = {"objetivo": objetivo, "hallazgo": hallazgo, "decision": decision, "responsable": responsable,
            "efecto": efecto}
    vacias = [k for k, v in fila.items() if not str(v).strip()]
    if vacias:
        raise ValueError(f"Fila de trazabilidad incompleta, faltan: {vacias}")
    return fila


def _usd(x: float) -> str:
    return f"US$ {x / 1e6:,.2f} M" if abs(x) >= 1e6 else f"US$ {x:,.0f}"


# --------------------------------------------------------------------------- Spark: datos

def linea_base(spark: SparkSession, lk: Lake, anio: int | None = None) -> dict:
    """Salidas y llegadas con >= 15 min y cancelaciones de LATAM pasajeros en el último año completo de silver.

    La tasa de llegadas >= 15 se calcula para poder compararla con el OTP de Cirium, que se mide en llegadas
    (y sobre todo el grupo, no solo Brasil): no son la misma población ni la misma definición.
    """
    v = spark.read.parquet(lk.ruta("silver_vuelos")).where(F.col("es_latam") & F.col("empresa_icao").isin(empresas_modelo()))
    anios = sorted(r["year"] for r in v.select("year").distinct().collect())
    if not anios:
        raise RuntimeError("silver/vuelos está vacío")
    anio = anio if anio in anios else (2025 if 2025 in anios else anios[-1])
    d = v.where(F.col("year") == anio)
    tot = d.count()
    oper = d.where(~F.col("cancelado") & ~F.col("es_atipico") & F.col("demora_salida_min").isNotNull())
    r = oper.agg(F.count("*").alias("n"), F.avg((F.col("demora_salida_min") >= 15).cast("double")).alias("t15"),
                 F.avg((F.col("demora_salida_min") >= 60).cast("double")).alias("t60"),
                 F.avg("demora_salida_min").alias("media"),
                 F.avg(F.when(F.col("demora_llegada_min").isNotNull(),
                              (F.col("demora_llegada_min") >= 15).cast("double"))).alias("l15")).first()
    canc = d.where(F.col("cancelado")).count()
    cfg = cargar_yaml("pipeline")["valor_negocio"]["linea_base"]
    return {"anio": anio, "empresas": ",".join(empresas_modelo()), "vuelos_programados": tot,
            "vuelos_operados": int(r["n"]), "tasa_salida_15": round(float(r["t15"] or 0), 4),
            "tasa_salida_60": round(float(r["t60"] or 0), 4), "tasa_llegada_15": round(float(r["l15"] or 0), 4),
            "puntualidad_llegada_vra": round(1 - float(r["l15"] or 0), 4),
            "demora_media_min": round(float(r["media"] or 0), 2), "cancelados": canc,
            "tasa_cancelacion": round(canc / max(tot, 1), 4),
            "otp_llegada_cirium": cfg["otp_cirium_2025"], "fuente_otp": cfg["fuente_otp"]}


def _pasajeros(spark: SparkSession, lk: Lake) -> DataFrame | None:
    try:
        return spark.read.parquet(lk.ruta("silver_pasajeros")).select(
            "empresa_icao", "origen_icao", "destino_icao", "year", "month", F.col("pax_por_despegue").alias("pax_estimados"))
    except AnalysisException:
        _log.warning("no hay silver/pasajeros_ruta_mes: los minutos-pasajero quedan en 0")
        return None


def _datos_evaluacion(spark: SparkSession, lk: Lake) -> tuple[DataFrame, bool]:
    """Features (solo empresas de pasajeros) + predicciones de valid y test + pasajeros estimados por vuelo."""
    try:
        pred = spark.read.parquet(lk.ruta("gold_predicciones")).select("id_vuelo", "prob_demora15", "prob_demora60")
    except AnalysisException as exc:
        raise RuntimeError("No hay gold/predicciones: primero hay que entrenar y predecir") from exc
    cols = ["id_vuelo", "split", "empresa_icao", "origen_icao", "destino_icao", "hora_local", "partida_prevista_utc",
            "tasa_hist_origen_hora", "o_categoria_vuelo", "o_fen_niebla", "o_fen_neblina", "o_fen_lluvia",
            "o_fen_tormenta", "red_frac_atrasadas", "y_demora15", "y_demora60", "demora_salida_min", "year", "month"]
    feats = cargar_features(spark, lk)
    feats = feats.select(*[c for c in cols if c in feats.columns]).where(F.col("empresa_icao").isin(empresas_modelo()))
    datos = feats.join(pred, "id_vuelo")
    pax = _pasajeros(spark, lk)
    if pax is None:
        return datos.withColumn("pax_estimados", F.lit(None).cast("double")), False
    return datos.join(pax, ["empresa_icao", "origen_icao", "destino_icao", "year", "month"], "left"), True


def calidad_alerta_roja(spark: SparkSession, lk: Lake) -> list[dict]:
    """Precisión y recall de la alerta roja publicada (nivel_alerta = rojo) por split, con y sin LATAM Cargo.

    Se reporta con carga para que se vea cuánto de la cifra anterior venía de esos vuelos: los cargueros salen
    con 60 min o más mucho más seguido y el modelo los marcaba casi a todos.
    """
    f = cargar_features(spark, lk).select("id_vuelo", "empresa_icao", "split", "y_demora60")
    p = spark.read.parquet(lk.ruta("gold_predicciones")).select("id_vuelo", "nivel_alerta")
    d = f.join(p, "id_vuelo")
    filas = []
    for poblacion, df in (("grupo_con_carga", d), ("solo_pasajeros", d.where(F.col("empresa_icao").isin(empresas_modelo())))):
        alerta = F.col("nivel_alerta") == "rojo"
        for r in (df.groupBy("split").agg(F.count("*").alias("vuelos"), F.sum("y_demora60").alias("demoras60"),
                                          F.sum(alerta.cast("int")).alias("alertas"),
                                          F.sum((alerta & (F.col("y_demora60") == 1)).cast("int")).alias("vp"))
                  .orderBy("split").collect()):
            filas.append({"poblacion": poblacion, "split": r["split"], "vuelos": r["vuelos"],
                          "demoras60": int(r["demoras60"] or 0), "alertas_rojas": int(r["alertas"] or 0),
                          "vp": int(r["vp"] or 0), "precision": round((r["vp"] or 0) / max(r["alertas"] or 0, 1), 4),
                          "recall": round((r["vp"] or 0) / max(r["demoras60"] or 0, 1), 4)})
    return filas


def _sensibilidad(df_eval: DataFrame, df_valid: DataFrame, objetivo: str, col_prob: str,
                  razones: list[float]) -> list[dict]:
    """Umbrales elegidos en validación para cada razón C_FN/C_FP y su resultado en el split evaluado."""
    umbrales = elegir(curva(df_valid.withColumnRenamed(col_prob, "prob"), objetivo), razones)
    dias = df_eval.select(F.to_date("partida_prevista_utc").alias("d")).distinct().count()
    filas = []
    for u in umbrales:
        y = F.col(objetivo).cast("int")
        a = (F.col(col_prob) >= u["umbral"]).cast("int")
        m = df_eval.agg(F.sum(a * y).alias("vp"), F.sum(a * (1 - y)).alias("fp"), F.sum((1 - a) * y).alias("fn"),
                        F.sum((1 - a) * (1 - y)).alias("vn")).first()
        vp, fp, fn, vn = (int(m[k] or 0) for k in ("vp", "fp", "fn", "vn"))
        filas.append({"objetivo": objetivo, "razon_cfn_cfp": u["razon_cfn_cfp"], "umbral": u["umbral"], "vp": vp,
                      "fp": fp, "fn": fn, "vn": vn, "precision": round(vp / max(vp + fp, 1), 4),
                      "recall": round(vp / max(vp + fn, 1), 4),
                      "alertas_por_dia": round((vp + fp) / max(dias, 1), 1),
                      "tasa_alerta": round((vp + fp) / max(vp + fp + fn + vn, 1), 4)})
    return filas


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


# --------------------------------------------------------------------------- tablas de valor

def _fila_politica(prod: str, regla: str, ambito: str, pol: str, sumas: dict, escenarios: list[pa.Supuestos]) -> dict:
    t = pa.totalizar(sumas, pol) if pol != "sin_sistema" else pa.Totales(int(sumas["dias"]), sumas["vuelos"],
                                                                         sumas["positivos"], 0.0, 0.0)
    fila = {"producto": prod, "regla": regla, "ambito": ambito, "politica": pol, "dias": t.dias,
            "vuelos": int(t.vuelos), "demoras": int(t.positivos), "alertas": round(t.alertas),
            "alertas_por_dia": round(t.alertas_por_dia, 1), "vp": round(t.vp, 1), "precision": round(t.precision, 4),
            "recall": round(t.recall, 4)}
    for s in escenarios:
        fila[f"beneficio_{s.nombre}_usd"] = 0.0 if pol == "sin_sistema" else round(pa.beneficio_politica(sumas, pol, s), 0)
    return fila


def tablas_valor(filas: list[dict], reglas: list[str], escenarios: list[pa.Supuestos], cfg: dict,
                 costos_minuto: dict[str, float], grilla_m: list[int], con_pasajeros: bool) -> dict[str, list[dict]]:
    """Arma valor_incremental, capacidad_topk, punto_equilibrio, intervalos_bootstrap y valor_escenarios."""
    base = next(s for s in escenarios if s.nombre == "base")
    bs = cfg["bootstrap"]
    incremental, capacidad, equilibrio, intervalos, esc_rows = [], [], [], [], []
    for prod in pa.PRODUCTOS:
        for regla in reglas:
            for ambito, grupos in pa.AMBITOS.items():
                dias = pa.serie_diaria(filas, prod, regla, grupos)
                if not dias:
                    continue
                s = pa.sumar(dias)
                for pol in ("sin_sistema", *pa.POLITICAS):
                    fila = _fila_politica(prod, regla, ambito, pol, s, escenarios)
                    if pol == "modelo":
                        ref = pa.totalizar(s, "referencia")
                        fila["vp_extra_vs_referencia"] = round(s["modelo_vp"] - ref.vp, 1)
                        fila["minutos_extra_vs_referencia_base"] = round(
                            s.get(f"modelo_ah{base.minutos_por_accion}", 0) - s.get(f"referencia_ah{base.minutos_por_accion}", 0), 0)
                        for e in escenarios:
                            fila[f"incremento_vs_referencia_{e.nombre}_usd"] = round(
                                pa.beneficio_politica(s, "modelo", e) - pa.beneficio_politica(s, "referencia", e), 0)
                    incremental.append(fila)
                # punto de equilibrio
                for pol in ("referencia", "modelo"):
                    curva_m = {m: s.get(f"{pol}_ah{m}", 0.0) for m in grilla_m}
                    n = s.get(f"{pol}_alertas", 0.0)
                    for clave_c, c in costos_minuto.items():
                        for a in cfg["costos_accion_equilibrio"]:
                            m_eq = pa.minutos_equilibrio(curva_m, n, c, float(a))
                            f_eq = pa.fraccion_equilibrio(s.get(f"{pol}_min_tope", 0.0), n, c, float(a))
                            equilibrio.append({
                                "producto": prod, "regla": regla, "ambito": ambito, "politica": pol,
                                "costo_minuto": clave_c, "costo_minuto_usd": c, "costo_accion_usd": a,
                                "alertas": round(n), "minutos_por_accion_equilibrio": None if m_eq is None else round(m_eq, 2),
                                "fraccion_equilibrio": None if f_eq is None else round(f_eq, 4),
                                "plausible": bool(f_eq is not None and f_eq <= TECHO_REACCIONARIA)})
                # bootstrap con el escenario base
                ic = pa.bootstrap(dias, pa.estadisticos_politica(base), bs["repeticiones"], bs["semilla"], bs["confianza"])
                for k, v in ic.items():
                    intervalos.append({"producto": prod, "regla": regla, "ambito": ambito, "estadistico": k,
                                       "puntual": round(v["puntual"], 4), "p2_5": round(v["p_inf"], 4),
                                       "mediana": round(v["mediana"], 4), "p97_5": round(v["p_sup"], 4)})
                # capacidad y escenarios (solo modelo)
                ref = pa.totalizar(s, "referencia")
                mod = pa.totalizar(s, "modelo")
                for e in escenarios:
                    m = e.minutos_por_accion
                    rec15 = s.get(f"modelo_rec15_{m}", 0.0)
                    rec60 = s.get(f"modelo_rec60_{m}", 0.0)
                    fila = {"producto": prod, "regla": regla, "ambito": ambito, "escenario": e.nombre,
                            "minutos_por_accion": m, "costo_minuto_usd": e.costo_minuto_usd,
                            "costo_accion_usd": e.costo_accion_usd, "alertas": round(mod.alertas),
                            "alertas_por_dia": round(mod.alertas_por_dia, 1), "vp": round(mod.vp),
                            "precision": round(mod.precision, 4), "recall": round(mod.recall, 4),
                            "precision_referencia": round(ref.precision, 4), "recall_referencia": round(ref.recall, 4),
                            "minutos_ahorrados": round(s.get(f"modelo_ah{m}", 0.0)),
                            "ahorro_usd": round(s.get(f"modelo_ah{m}", 0.0) * e.costo_minuto_usd),
                            "costo_acciones_usd": round(mod.alertas * e.costo_accion_usd),
                            "beneficio_neto_usd": round(pa.beneficio_politica(s, "modelo", e)),
                            "beneficio_referencia_usd": round(pa.beneficio_politica(s, "referencia", e)),
                            "incremento_vs_referencia_usd": round(pa.beneficio_politica(s, "modelo", e)
                                                                  - pa.beneficio_politica(s, "referencia", e)),
                            "vuelos": int(s["vuelos"]), "tasa15_antes": round(s["pos15"] / s["vuelos"], 4),
                            "tasa15_despues": round(pa.tasa_tras_actuar(s["pos15"], rec15, s["vuelos"]), 4),
                            "recuperados15": round(rec15), "tasa60_antes": round(s["pos60"] / s["vuelos"], 4),
                            "tasa60_despues": round(pa.tasa_tras_actuar(s["pos60"], rec60, s["vuelos"]), 4),
                            "recuperados60": round(rec60),
                            "pasajeros_bajo_2h": round(s.get(f"modelo_pax120_{m}", 0.0)) if con_pasajeros else None,
                            "minutos_pasajero_ahorrados": round(s.get(f"modelo_paxmin{m}", 0.0)) if con_pasajeros else None}
                    if regla.startswith(("tope", "cuota")):
                        k = int(re.sub(r"\D", "", regla))
                        capacidad.append({"k_por_aeropuerto_dia": k, **fila})
                    if ambito == "red":
                        esc_rows.append(fila)
    return {"valor_incremental": incremental, "capacidad_topk": capacidad, "punto_equilibrio": equilibrio,
            "intervalos_bootstrap": intervalos, "valor_escenarios": esc_rows}


def _buscar(filas: list[dict], **filtro) -> dict:
    return next((f for f in filas if all(f.get(k) == v for k, v in filtro.items())), {})


def trazabilidad(base: dict, t: dict[str, list[dict]], hall: dict, tasa_eval: float, periodo: str,
                 principal: tuple[str, str], complementario: tuple[str, str]) -> list[dict]:
    """Tabla 6.1 con cifras del pipeline. Las filas sin mecanismo cuantificado se rotulan como cota superior."""
    filas = []
    textos = {"ambar": "15 min o más (ámbar)", "rojo": "60 min o más (rojo)"}

    def regla_txt(regla: str) -> str:
        if regla.startswith("tope"):
            return f"las alertas sobre el umbral, con un máximo de {regla[4:]} por aeropuerto y día"
        return "todas las alertas sobre el umbral"

    for prod, regla in (principal, complementario):
        r = _buscar(t["capacidad_topk"] + t["valor_escenarios"], producto=prod, regla=regla, ambito="red", escenario="base")
        inc = _buscar(t["intervalos_bootstrap"], producto=prod, regla=regla, ambito="red",
                      estadistico="incremento_modelo_vs_referencia")
        eq = _buscar(t["punto_equilibrio"], producto=prod, regla=regla, ambito="red", politica="modelo",
                     costo_minuto="a4a_sin_combustible", costo_accion_usd=150)
        eq_br = _buscar(t["punto_equilibrio"], producto=prod, regla=regla, ambito="red", politica="modelo",
                        costo_minuto="brasil_operacional", costo_accion_usd=150)
        filas.append(fila_trazabilidad(
            f"Avisar a T-2 h los vuelos con riesgo de salir con {textos[prod]}",
            f"Con {regla_txt(regla)} ({r.get('alertas_por_dia')} por día) el modelo acierta "
            f"{r.get('precision', 0) * 100:.1f} % frente a {r.get('precision_referencia', 0) * 100:.1f} % de una tabla de "
            f"tasas por aeropuerto y hora con las mismas alertas ({periodo}, solo pasajeros)",
            "Entregar la lista al jefe de turno a T-2 h para decidir la acción (reserva de avión o tripulación, prioridad "
            "de rampa, re-protección de conexiones, aviso a pasajeros)",
            "Jefe de turno del CCO de LATAM Brasil",
            f"Atribuible al modelo y no a la tabla: {_usd(inc.get('puntual', 0))} al año (IC 95 % {_usd(inc.get('p2_5', 0))} "
            f"a {_usd(inc.get('p97_5', 0))}) en el escenario base. La lista se paga si cada acción ahorra al menos "
            f"{eq.get('minutos_por_accion_equilibrio')} min (US$ 69/min) o {eq_br.get('minutos_por_accion_equilibrio')} "
            f"min (US$ 28,9/min). Salidas con 15 min o más: {r.get('tasa15_antes', 0) * 100:.2f} % -> "
            f"{r.get('tasa15_despues', 0) * 100:.2f} % si cada acción ahorra {r.get('minutos_por_accion')} min"))
    prod, regla = principal
    for hub in ("SBGR", "SBSP"):
        h = _buscar(t["capacidad_topk"] + t["valor_escenarios"], producto=prod, regla=regla, ambito=hub, escenario="base")
        hi = _buscar(t["intervalos_bootstrap"], producto=prod, regla=regla, ambito=hub,
                     estadistico="incremento_modelo_vs_referencia")
        if not h:
            continue
        sin_ventaja = hi.get("p2_5", 0) <= 0
        filas.append(fila_trazabilidad(
            f"Usar la alerta {textos[prod].split('(')[1].rstrip(')')} en el hub {hub}",
            f"En {hub} la lista acierta {h.get('precision', 0) * 100:.1f} % y la tabla de tasas {h.get('precision_referencia', 0) * 100:.1f} %"
            f" con las mismas alertas; diferencia de valor {_usd(hi.get('puntual', 0))} (IC 95 % {_usd(hi.get('p2_5', 0))} a "
            f"{_usd(hi.get('p97_5', 0))})",
            ("En este hub el modelo no le gana a la tabla: usar la tabla por hora (más simple) y mejorar el modelo con la "
             "rotación de aviones antes de adoptarlo" if sin_ventaja else
             f"Adoptar la lista del modelo en la reunión de turno de {hub}"),
            f"Coordinador de hub del CCO en {hub}",
            f"Beneficio neto de actuar sobre la lista en el escenario base: {_usd(h.get('beneficio_neto_usd', 0))} al año"))
    if hall["franjas"]:
        f = hall["franjas"][0]
        exceso = int(f["vuelos"] * max(f["tasa_demora15"] - tasa_eval, 0))
        filas.append(fila_trazabilidad(
            "Ajustar la programación donde la demora se acumula",
            f"La franja {f['grupo']} h tiene {f['tasa_demora15'] * 100:.1f} % de salidas con 15 min o más "
            f"(red: {tasa_eval * 100:.1f} %)",
            f"Estudiar más holgura de giro en las rotaciones de la franja {f['grupo']} h, con su costo en utilización",
            "Planeamiento de red y programación",
            f"Cota superior, no estimación: {exceso} salidas menos con 15 min o más si la franja llegara al promedio; "
            "el efecto real depende de cuánta holgura se agregue y no se modeló"))
    clima = [c for c in hall["clima"] if c["grupo"] not in ("VFR/MVFR sin fenómenos", "sin METAR")]
    vfr = next((c for c in hall["clima"] if c["grupo"] == "VFR/MVFR sin fenómenos"), None)
    if clima and vfr:
        c = clima[0]
        filas.append(fila_trazabilidad(
            "Anticipar el efecto del clima reportado en el METAR",
            f"Con {c['grupo']} en el origen a T-2 h la tasa es {c['tasa_demora15'] * 100:.1f} % frente a "
            f"{vfr['tasa_demora15'] * 100:.1f} % con tiempo VFR ({c['vuelos']} vuelos en el periodo)",
            f"Protocolo de clima: si el METAR de T-2 h reporta {c['grupo']}, esos vuelos entran a la lista de revisión",
            "Despacho de vuelos y meteorología del CCO",
            f"Cota superior: {c['vuelos']} salidas con riesgo {c['tasa_demora15'] / max(vfr['tasa_demora15'], 1e-9):.1f} "
            "veces mayor; ya están dentro del puntaje del modelo, no es un ahorro adicional"))
    if hall["red"]:
        r = hall["red"][0]
        filas.append(fila_trazabilidad(
            "Cortar la propagación de demoras en la red",
            f"Cuando {r['grupo']} de las salidas previas del origen ya iban atrasadas, la tasa sube a "
            f"{r['tasa_demora15'] * 100:.1f} %",
            "Si en [T-4 h, T-2 h) más del 30 % de las salidas del aeropuerto van atrasadas, escalar al coordinador del hub",
            "Coordinador de hub del CCO",
            f"Cota superior: {r['vuelos']} salidas del periodo en esa condición; el CCO ya observa este estado en "
            "tiempo real, el aporte es avisarlo de forma sistemática"))
    filas.append(fila_trazabilidad(
        "Mantener la calidad de la alerta en el tiempo",
        "Las métricas cambian por temporada y la red cambia (deriva medida con PSI)",
        "Revisar cada mes el KPI K4 (precisión y recall de la alerta) y reentrenar si la precisión de la lista "
        "cae por debajo de la de la tabla de tasas",
        "Analítica de operaciones",
        f"Sostener el efecto sobre la línea base de {base['tasa_salida_15'] * 100:.1f} % de salidas con 15 min o más"))
    return filas


def _escribir_csv(directorio: Path, nombre: str, filas: list[dict]) -> None:
    if not filas:
        return
    directorio.mkdir(parents=True, exist_ok=True)
    campos: list[str] = []
    for f in filas:
        campos += [k for k in f if k not in campos]
    with open(directorio / nombre, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        w.writerows(filas)


# --------------------------------------------------------------------------- orquestación

def ejecutar(split: str = "test") -> dict:
    """Calcula y publica el valor de negocio (CSV en docs/evidencias/valor y JSON en gold/metricas)."""
    cfg_total = cargar_yaml("pipeline")
    cfg = cfg_total["valor_negocio"]
    lk = lake()
    spark = crear_sesion("valor_negocio")
    with registrar_corrida("valor_negocio") as info:
        base = linea_base(spark, lk)
        datos, con_pax = _datos_evaluacion(spark, lk)
        datos = datos.cache()
        splits = {r["split"] for r in datos.select("split").distinct().collect()}
        if split not in splits:
            raise RuntimeError(f"No hay predicciones de {split}")
        df_eval = datos.where(F.col("split") == split)
        df_valid = datos.where(F.col("split") == "valid")
        tasa_train = float(cargar_features(spark, lk).where("split = 'train'").agg(F.avg("y_demora15")).first()[0] or 0.2)
        total = df_eval.count()
        tasa_eval = float(df_eval.agg(F.avg(F.col("y_demora15").cast("double"))).first()[0] or 0)
        meses = sorted({f"{r['year']}-{r['month']:02d}" for r in df_eval.select("year", "month").distinct().collect()})
        periodo = f"{split} {meses[0]}..{meses[-1]}"
        razones = [float(x) for x in cfg_total["modelo"]["razones_cfn_cfp"]]
        sens = (_sensibilidad(df_eval, df_valid, "y_demora15", "prob_demora15", razones)
                + _sensibilidad(df_eval, df_valid, "y_demora60", "prob_demora60", razones))
        razon = float(cfg["razon_principal"])
        umbrales = {"ambar": next(f["umbral"] for f in sens if f["objetivo"] == "y_demora15" and f["razon_cfn_cfp"] == razon),
                    "rojo": next(f["umbral"] for f in sens if f["objetivo"] == "y_demora60" and f["razon_cfn_cfp"] == razon)}
        escenarios = supuestos_desde_config(cfg)
        costos_minuto = {k: float(v["usd"]) for k, v in cfg["costos_minuto"].items()}
        grilla_m = sorted(set(int(m) for m in cfg["grilla_minutos_por_accion"]) | {e.minutos_por_accion for e in escenarios})
        m_esc = sorted({e.minutos_por_accion for e in escenarios})
        ks = [int(k) for k in cfg["capacidad_cco"]["k_por_aeropuerto_dia"]]
        top10 = [r["origen_icao"] for r in
                 df_eval.groupBy("origen_icao").count().orderBy(F.desc("count")).limit(10).collect()]
        d = pa.preparar(df_eval, tasa_train, cfg_total["zona_horaria_vra"], top10, int(cfg["tope_minutos"]), m_esc)
        d, claves = pa.banderas(d, umbrales, ks)
        filas = pa.agregar(d, claves, grilla_m, m_esc, con_pax)
        reglas = ["umbral"] + [f"tope{k}" for k in ks] + [f"cuota{k}" for k in ks]
        t = tablas_valor(filas, reglas, escenarios, cfg, costos_minuto, grilla_m, con_pax)
        hall = hallazgos(df_eval)
        principal = (cfg["producto_principal"], cfg["regla_principal"])
        complementario = (cfg["producto_complementario"], cfg["regla_complementaria"])
        traz = trazabilidad(base, t, hall, tasa_eval, periodo, principal, complementario)
        cobertura_pax = (df_eval.agg(F.avg(F.col("pax_estimados").isNotNull().cast("double"))).first()[0]
                         if con_pax else 0.0)
        resumen = {"periodo_evaluado": periodo, "vuelos_evaluados": total, "tasa_demora15_evaluada": round(tasa_eval, 4),
                   "linea_base": base, "umbrales": umbrales, "top10_aeropuertos": top10,
                   "cobertura_pasajeros": round(float(cobertura_pax or 0), 4),
                   "supuestos": {k: cfg[k] for k in ("costos_minuto", "escenarios", "nota_supuestos")},
                   "principal": _buscar(t["capacidad_topk"] + t["valor_escenarios"], producto=principal[0],
                                        regla=principal[1], ambito="red", escenario="base"),
                   "trazabilidad": traz, "hallazgos": hall}
        salida = ruta_repo(cfg_total["evidencias"]["dir_local"], "valor")
        _escribir_csv(salida, "linea_base.csv", [base])
        _escribir_csv(salida, "sensibilidad_umbral.csv", sens)
        for nombre, tabla in t.items():
            _escribir_csv(salida, f"{nombre}.csv", tabla)
        _escribir_csv(salida, "trazabilidad.csv", traz)
        _escribir_csv(salida, "calidad_alerta_roja.csv", calidad_alerta_roja(spark, lk))
        for k, fh in hall.items():
            _escribir_csv(salida, f"hallazgos_{k}.csv", fh)
        Almacen(lk).escribir_json(f"{lk.relativa('gold_metricas')}/valor_negocio.json", resumen)
        datos.unpersist()
        info["filas_entrada"] = total
        p = resumen["principal"]
        info["detalle"] = (f"{principal[0]} {principal[1]}: beneficio base {p.get('beneficio_neto_usd')} US$, "
                           f"incremento vs referencia {p.get('incremento_vs_referencia_usd')} US$")
        _log.info(f"valor de negocio ({periodo}): {info['detalle']}")
        return resumen


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Valor de negocio de la alerta")
    p.add_argument("--split", default="test")
    ejecutar(p.parse_args(argv).split)


if __name__ == "__main__":
    main()
