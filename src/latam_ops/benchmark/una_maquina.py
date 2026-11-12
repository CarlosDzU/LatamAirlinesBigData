"""E6: el mismo cálculo central de features en UNA sola máquina con pandas (sin Spark), para comparar.

Responde a la pregunta de la guía: ¿los datos caben y rinden en una sola máquina, hoy y cuando crezcan?
Calcula congestión programada y estado de la red a T-2 h para los vuelos de un año y mide tiempo y memoria pico.

Uso: python -m latam_ops.benchmark.una_maquina --anio 2024 [--factor 1]
"""

from __future__ import annotations

import argparse
import resource
import time

import pandas as pd

from latam_ops.benchmark.escalabilidad import guardar
from latam_ops.comun.config import lake


def ejecutar(anio: int, factor: int) -> dict:
    lk = lake()
    ruta = f"{lk.base_boto}/{lk.relativa('silver_vuelos')}" if not lk.es_s3 else f"s3://{lk.base_boto}/{lk.relativa('silver_vuelos')}"
    fila = {"escalon": f"E6x{factor}", "nodos": 0, "factor": factor, "desde": f"{anio}-01", "hasta": f"{anio}-12", "estado": "ok"}
    t0 = time.time()
    try:
        cols = ["id_vuelo", "es_latam", "origen_icao", "partida_prevista_local", "partida_prevista_utc", "partida_real_utc",
                "demora_salida_min", "cancelado", "year"]
        df = pd.read_parquet(ruta, columns=cols, filters=[("year", "=", anio)])
        if factor > 1:
            df = pd.concat([df.assign(id_vuelo=df["id_vuelo"] + f"_r{k}") for k in range(factor)], ignore_index=True)
        fila["filas_entrada"] = len(df)
        df["hl"] = df["partida_prevista_local"].dt.floor("h")
        cong = df.groupby(["origen_icao", "hl"]).size().rename("congestion_hora_origen")
        df = df.join(cong, on=["origen_icao", "hl"])
        df["kr"] = (df["partida_prevista_utc"] - pd.Timedelta(hours=2)).dt.floor("h")
        sal = df[df["partida_real_utc"].notna() & ~df["cancelado"]].copy()
        sal["h"] = sal["partida_real_utc"].dt.floor("h")
        agg = sal.groupby(["origen_icao", "h"]).agg(n=("id_vuelo", "size"), s=("demora_salida_min", "sum")).reset_index()
        feats = []
        for desfase in (1, 2):
            tmp = df[["id_vuelo", "origen_icao", "kr"]].assign(h=df["kr"] - pd.Timedelta(hours=desfase))
            feats.append(tmp.merge(agg, on=["origen_icao", "h"], how="left")[["id_vuelo", "n", "s"]])
        red = pd.concat(feats).groupby("id_vuelo")[["n", "s"]].sum()
        out = df[df["es_latam"]].join(red, on="id_vuelo")
        fila["filas_salida"] = len(out)
    except MemoryError as exc:
        fila["estado"], fila["detalle"] = "OOM", str(exc)[:200]
    fila["segundos_pipeline"] = round(time.time() - t0, 2)
    fila["pico_heap_executor_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    guardar(fila)
    return fila


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="E6: una sola máquina (pandas)")
    p.add_argument("--anio", type=int, default=2024)
    p.add_argument("--factor", type=int, default=1)
    a = p.parse_args(argv)
    ejecutar(a.anio, a.factor)


if __name__ == "__main__":
    main()
