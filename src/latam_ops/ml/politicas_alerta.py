"""Comparación de políticas de alerta con el mismo presupuesto de acciones del CCO.

La pregunta que responde: ¿cuánto valor agrega el modelo frente a lo que el CCO podría hacer sin él?
Con la misma cantidad de vuelos revisados por aeropuerto y día se comparan:
- sin sistema: no se actúa (beneficio 0, punto de partida);
- aleatoria: se revisan vuelos al azar en el mismo aeropuerto (valor esperado, sirve de piso);
- referencia: se priorizan por la tasa histórica de demora del origen a esa hora (calculada solo con train);
- modelo: se priorizan por la probabilidad del Random Forest.
El valor atribuible al proyecto es modelo - referencia: la referencia sale de una tabla dinámica y no
necesita ni Big Data ni ML.

Reglas de alerta:
- umbral: el modelo alerta cuando la probabilidad supera el umbral elegido en validación;
- tope{K}: igual, pero el jefe de turno actúa a lo sumo sobre K vuelos por aeropuerto y día;
- cuota{K}: se revisan siempre los K vuelos de mayor riesgo de cada aeropuerto y día.
La referencia recibe el mismo número de alertas por aeropuerto en el periodo (ver `banderas`).

Cómo se valora una acción: una acción preventiva a T-2 h (avión o tripulación de reserva, adelantar el
embarque, priorizar la rampa) ahorra a lo sumo m minutos, nunca más que la demora que tuvo el vuelo:
ahorro = min(demora, m). Así una demora de 3 h por una falla técnica no cuenta como 36 min evitados.

La contabilidad se hace en Spark agrupando por (día, grupo de aeropuertos) y luego en Python sobre unas
1 500 filas, lo que permite el bootstrap por día sin volver a tocar el detalle de vuelos.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# producto -> (etiqueta, columna de probabilidad)
PRODUCTOS = {"ambar": ("y_demora15", "prob_demora15"), "rojo": ("y_demora60", "prob_demora60")}
POLITICAS = ("aleatoria", "referencia", "modelo")
GRUPOS = ("SBGR", "SBSP", "otros_top10", "resto")
# ámbito -> grupos que lo forman
AMBITOS = {"red": GRUPOS, "SBGR": ("SBGR",), "SBSP": ("SBSP",), "top10": ("SBGR", "SBSP", "otros_top10"),
           "resto": ("resto",)}


# --------------------------------------------------------------------------- cuentas puras (sin Spark)

@dataclass(frozen=True)
class Supuestos:
    """Supuestos económicos de un escenario."""

    nombre: str
    minutos_por_accion: int     # tope de minutos que una acción a T-2 h puede ahorrar
    costo_minuto_usd: float     # costo para la aerolínea de un minuto de demora en tierra
    costo_accion_usd: float     # costo de revisar y actuar sobre un vuelo alertado (acierto o no)


def beneficio(ahorro_minutos: float, alertas: float, costo_minuto: float, costo_accion: float) -> float:
    """Ahorro por la demora evitada menos el costo de actuar sobre cada alerta."""
    return ahorro_minutos * costo_minuto - alertas * costo_accion


def fraccion_equilibrio(minutos: float, alertas: float, costo_minuto: float, costo_accion: float) -> float | None:
    """Fracción de los minutos alertados que hay que evitar para no perder: f* = a·N / (c·minutos)."""
    if minutos <= 0 or costo_minuto <= 0:
        return None
    return costo_accion * alertas / (costo_minuto * minutos)


def minutos_equilibrio(curva: dict[int, float], alertas: float, costo_minuto: float, costo_accion: float) -> float | None:
    """Minutos por acción (m*) a partir de los cuales la política se paga.

    curva: m -> sum(min(demora, m)) de los vuelos alertados (creciente en m). Interpola linealmente entre los
    puntos de la grilla; None si ni con el mayor m de la grilla alcanza.
    """
    objetivo = costo_accion * alertas / costo_minuto if costo_minuto > 0 else float("inf")
    puntos = sorted(curva.items())
    previo_m, previo_v = 0, 0.0
    for m, v in puntos:
        if v >= objetivo:
            if v == previo_v:
                return float(m)
            return previo_m + (objetivo - previo_v) * (m - previo_m) / (v - previo_v)
        previo_m, previo_v = m, v
    return None


def tasa_tras_actuar(positivos: float, recuperados: float, vuelos: float) -> float:
    """Tasa de salidas sobre el umbral si los vuelos recuperados bajan de él."""
    return max(positivos - recuperados, 0.0) / vuelos if vuelos else 0.0


def sumar(dias: list[dict]) -> dict[str, float]:
    """Suma todas las columnas numéricas de una serie diaria (agrega 'dias' con el número de días)."""
    sumas: dict[str, float] = defaultdict(float)
    for d in dias:
        for k, v in d.items():
            if k != "fecha":
                sumas[k] += v or 0
    sumas["dias"] = len(dias)
    return dict(sumas)


@dataclass(frozen=True)
class Totales:
    """Totales de una política sobre un conjunto de días."""

    dias: int
    vuelos: float
    positivos: float
    alertas: float
    vp: float

    @property
    def precision(self) -> float:
        return self.vp / self.alertas if self.alertas else 0.0

    @property
    def recall(self) -> float:
        return self.vp / self.positivos if self.positivos else 0.0

    @property
    def alertas_por_dia(self) -> float:
        return self.alertas / self.dias if self.dias else 0.0


def totalizar(sumas: dict[str, float], politica: str) -> Totales:
    """Totales de una política a partir de las sumas ('<politica>_alertas', '<politica>_vp')."""
    return Totales(dias=int(sumas["dias"]), vuelos=sumas["vuelos"], positivos=sumas["positivos"],
                   alertas=sumas.get(f"{politica}_alertas", 0.0), vp=sumas.get(f"{politica}_vp", 0.0))


def beneficio_politica(sumas: dict[str, float], politica: str, s: Supuestos) -> float:
    """Beneficio neto de una política con los supuestos de un escenario."""
    return beneficio(sumas.get(f"{politica}_ah{s.minutos_por_accion}", 0.0), sumas.get(f"{politica}_alertas", 0.0),
                     s.costo_minuto_usd, s.costo_accion_usd)


def bootstrap(dias: list[dict], estadisticos: Callable[[dict[str, float]], dict[str, float]],
              repeticiones: int = 1000, semilla: int = 42, confianza: float = 0.95) -> dict[str, dict[str, float]]:
    """Intervalo percentil remuestreando días completos con reposición.

    Se remuestrean días y no vuelos porque los vuelos de un mismo día comparten clima y estado de la red;
    remuestrear vuelos sueltos daría intervalos demasiado estrechos. Todas las políticas se miden sobre los
    mismos días, así que las diferencias (modelo - referencia) quedan pareadas.
    `estadisticos` recibe las sumas de la remuestra (como las de `sumar`).
    """
    claves = [k for k in dias[0] if k != "fecha"]
    x = np.array([[d.get(k) or 0.0 for k in claves] for d in dias], dtype=float)
    n = len(dias)
    sumas = x[np.random.default_rng(semilla).integers(0, n, size=(repeticiones, n))].sum(axis=1)
    muestras: dict[str, list[float]] = defaultdict(list)
    for fila in sumas:
        s = dict(zip(claves, fila, strict=True))
        s["dias"] = n
        for k, v in estadisticos(s).items():
            muestras[k].append(v)
    a = (1 - confianza) / 2
    puntual = estadisticos(sumar(dias))
    return {k: {"p_inf": float(np.quantile(v, a)), "mediana": float(np.quantile(v, 0.5)),
                "p_sup": float(np.quantile(v, 1 - a)), "puntual": puntual[k]} for k, v in muestras.items()}


def estadisticos_politica(s: Supuestos) -> Callable[[dict[str, float]], dict[str, float]]:
    """Para el bootstrap: precisión, recall y beneficio por política, y las diferencias del modelo."""
    def calc(sumas: dict[str, float]) -> dict[str, float]:
        out = {}
        for pol in POLITICAS:
            t = totalizar(sumas, pol)
            out[f"precision_{pol}"] = t.precision
            out[f"recall_{pol}"] = t.recall
            out[f"beneficio_{pol}"] = beneficio_politica(sumas, pol, s)
        out["incremento_modelo_vs_referencia"] = out["beneficio_modelo"] - out["beneficio_referencia"]
        out["incremento_modelo_vs_aleatoria"] = out["beneficio_modelo"] - out["beneficio_aleatoria"]
        return out
    return calc


# --------------------------------------------------------------------------- Spark

def preparar(df: DataFrame, tasa_train: float, zona_horaria: str, top10: list[str], tope_min: int,
             minutos_aleatoria: list[int]) -> DataFrame:
    """Fecha local, grupo de aeropuerto, minutos de demora, puntaje de la referencia, rankings y medias.

    Para la política aleatoria se guardan medias por aeropuerto en todo el periodo y por aeropuerto-día: la suma
    de esas medias sobre los vuelos que alertó el modelo es el valor esperado de elegir al azar la misma cantidad.
    """
    grupo = (F.when(F.col("origen_icao") == "SBGR", "SBGR").when(F.col("origen_icao") == "SBSP", "SBSP")
             .when(F.col("origen_icao").isin(top10), "otros_top10").otherwise("resto"))
    d = (df.withColumn("fecha_local", F.to_date(F.from_utc_timestamp("partida_prevista_utc", zona_horaria)))
         .withColumn("grupo", grupo)
         .withColumn("min_pos", F.greatest(F.col("demora_salida_min").cast("double"), F.lit(0.0)))
         .withColumn("min_tope", F.least(F.col("min_pos"), F.lit(float(tope_min))))
         .withColumn("s_ref", F.coalesce(F.col("tasa_hist_origen_hora").cast("double"), F.lit(float(tasa_train))))
         # desempate estable: la referencia tiene muchos empates (mismo origen y hora)
         .withColumn("desempate", F.xxhash64("id_vuelo")))
    ventanas = {"dia": Window.partitionBy("origen_icao", "fecha_local"), "aer": Window.partitionBy("origen_icao")}
    for sufijo, w in ventanas.items():
        d = (d.withColumn(f"media_y15_{sufijo}", F.avg(F.col("y_demora15").cast("double")).over(w))
             .withColumn(f"media_y60_{sufijo}", F.avg(F.col("y_demora60").cast("double")).over(w)))
        for m in minutos_aleatoria:
            d = d.withColumn(f"media_ah{m}_{sufijo}", F.avg(F.least(F.col("min_pos"), F.lit(float(m)))).over(w))
    for col in ("prob_demora15", "prob_demora60", "s_ref"):
        d = d.withColumn(f"rk_{col}", F.row_number().over(ventanas["dia"].orderBy(F.desc(col), "desempate")))
    return d


def banderas(d: DataFrame, umbrales: dict[str, float], ks: list[int]) -> tuple[DataFrame, list[tuple[str, str, str]]]:
    """Columnas booleanas de alerta por (producto, regla, política).

    Reglas:
    - umbral: el modelo alerta si prob >= umbral (sin tope);
    - tope{K}: igual, pero a lo sumo K vuelos por aeropuerto y día (capacidad del turno);
    - cuota{K}: exactamente los K vuelos de mayor riesgo de cada aeropuerto y día, haya o no riesgo.
    En umbral y tope la referencia recibe, en cada aeropuerto, el mismo número de alertas que el modelo en todo
    el periodo y las reparte con su propio puntaje (con el mismo tope diario). No se le da el reparto diario del
    modelo: eso le regalaría justo lo que el modelo sabe (qué días vienen malos).
    """
    claves = []
    por_aer = Window.partitionBy("origen_icao")
    for prod, (_, col_prob) in PRODUCTOS.items():
        sobre_umbral = F.col(col_prob) >= F.lit(float(umbrales[prod]))
        reglas = {"umbral": (sobre_umbral, F.lit(True))}
        for k in ks:
            reglas[f"tope{k}"] = (sobre_umbral & (F.col(f"rk_{col_prob}") <= k), F.col("rk_s_ref") <= k)
        for regla, (cond_modelo, elegible_ref) in reglas.items():
            am, ar = f"a_{prod}_{regla}_modelo", f"a_{prod}_{regla}_referencia"
            d = (d.withColumn(am, cond_modelo)
                 .withColumn("_n", F.sum(F.col(am).cast("int")).over(por_aer))
                 .withColumn("_eleg", elegible_ref)
                 .withColumn("_rk", F.row_number().over(por_aer.orderBy(F.desc("_eleg"), F.desc("s_ref"), "desempate")))
                 .withColumn(ar, F.col("_eleg") & (F.col("_rk") <= F.col("_n")))
                 .drop("_n", "_eleg", "_rk"))
            claves += [(prod, regla, "modelo"), (prod, regla, "referencia")]
        for k in ks:
            d = (d.withColumn(f"a_{prod}_cuota{k}_modelo", F.col(f"rk_{col_prob}") <= k)
                 .withColumn(f"a_{prod}_cuota{k}_referencia", F.col("rk_s_ref") <= k))
            claves += [(prod, f"cuota{k}", "modelo"), (prod, f"cuota{k}", "referencia")]
    return d, claves


def agregar(d: DataFrame, claves: list[tuple[str, str, str]], grilla_m: list[int], m_escenarios: list[int],
            con_pasajeros: bool) -> list[dict]:
    """Una fila por (día, grupo) con totales y, por bandera: alertas, aciertos, minutos y ahorro por m.

    rec15_m / rec60_m: vuelos alertados que bajarían de 15 / 60 min si la acción les quita m minutos.
    pax120_m: pasajeros de vuelos alertados que dejarían de pasar las 2 h de espera (umbral de alimentación de
    la Resolución ANAC 400). paxmin_m: minutos-pasajero ahorrados.
    """
    pax = F.coalesce(F.col("pax_estimados"), F.lit(0.0)) if con_pasajeros else F.lit(0.0)
    aggs = [F.count("*").alias("vuelos"), F.sum("min_tope").alias("min_tope_total"), F.sum(pax).alias("pax_total"),
            F.sum(F.col("y_demora15").cast("int")).alias("pos_ambar"),
            F.sum(F.col("y_demora60").cast("int")).alias("pos_rojo")]
    for prod, regla, pol in claves:
        etiqueta = PRODUCTOS[prod][0]
        a = F.col(f"a_{prod}_{regla}_{pol}")
        c = f"{prod}_{regla}_{pol}"

        def suma(expr, alias, a=a, c=c):
            return F.sum(F.when(a, expr).otherwise(F.lit(0.0))).alias(f"{c}_{alias}")

        aggs += [suma(F.lit(1.0), "alertas"), suma((F.col(etiqueta) == 1).cast("double"), "vp"),
                 suma(F.col("min_tope"), "min_tope")]
        aggs += [suma(F.least(F.col("min_pos"), F.lit(float(m))), f"ah{m}") for m in grilla_m]
        for m in m_escenarios:
            aggs += [suma(((F.col("min_pos") >= 15) & (F.col("min_pos") - m < 15)).cast("double"), f"rec15_{m}"),
                     suma(((F.col("min_pos") >= 60) & (F.col("min_pos") - m < 60)).cast("double"), f"rec60_{m}"),
                     suma(F.when((F.col("min_pos") >= 120) & (F.col("min_pos") - m < 120), pax).otherwise(0.0),
                          f"pax120_{m}"),
                     suma(pax * F.least(F.col("min_pos"), F.lit(float(m))), f"paxmin{m}")]
        if pol == "modelo":
            # valor esperado de la aleatoria con la misma cantidad de alertas por aeropuerto-día
            a_ = f"{prod}_{regla}_aleatoria"
            # en la cuota la cantidad diaria es fija, así que el azar se toma dentro del día; en las demás,
            # dentro del aeropuerto en todo el periodo (sin usar el reparto diario del modelo)
            sufijo = "dia" if regla.startswith("cuota") else "aer"
            col_media = f"media_y15_{sufijo}" if etiqueta == "y_demora15" else f"media_y60_{sufijo}"
            aggs += [F.sum(F.when(a, 1.0).otherwise(0.0)).alias(f"{a_}_alertas"),
                     F.sum(F.when(a, F.col(col_media)).otherwise(0.0)).alias(f"{a_}_vp")]
            aggs += [F.sum(F.when(a, F.col(f"media_ah{m}_{sufijo}")).otherwise(0.0)).alias(f"{a_}_ah{m}")
                     for m in m_escenarios]
    filas = d.groupBy("fecha_local", "grupo").agg(*aggs).collect()
    return [r.asDict() for r in filas]


def serie_diaria(filas: list[dict], prod: str, regla: str, grupos: tuple[str, ...]) -> list[dict]:
    """Serie por día de un ámbito, con claves sin prefijo de producto/regla: '<politica>_<medida>'."""
    prefijo = f"{prod}_{regla}_"
    por_dia: dict = {}
    for r in filas:
        if r["grupo"] not in grupos:
            continue
        d = por_dia.setdefault(r["fecha_local"], defaultdict(float))
        d["vuelos"] += r["vuelos"]
        d["positivos"] += r[f"pos_{prod}"]
        d["pos15"] += r["pos_ambar"]
        d["pos60"] += r["pos_rojo"]
        d["min_tope_total"] += r["min_tope_total"] or 0.0
        d["pax_total"] += r["pax_total"] or 0.0
        for k, v in r.items():
            if k.startswith(prefijo):
                d[k[len(prefijo):]] += v or 0.0
    salida = []
    for fecha in sorted(por_dia):
        d = dict(por_dia[fecha])
        d["fecha"] = fecha
        salida.append(d)
    return salida
