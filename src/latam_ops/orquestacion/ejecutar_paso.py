"""Punto de entrada único de cada paso del pipeline.

Lo usan Step Functions (vía SSM: scripts/ejecutar_paso.sh <paso> <objeto_s3>), el Makefile y la demo local.

Pasos de la máquina de estados (un archivo nuevo en bronze/landing/anac_vra/):
  validar  -> comprueba formato del archivo aterrizado y deduce el mes
  ingerir  -> lo copia a bronze/anac_vra/year=/month= con manifiesto sha256 y trae el METAR del año
  silver   -> silver/vuelos + silver/metar_obs + silver/clima_horario del mes
  gold     -> gold/features_salida del mes
  predecir -> gold/predicciones del mes con los modelos activos
  kpi      -> fact_vuelo + dimensiones + kpi_diario + kpi_alerta_mensual del mes
Sin objeto, los pasos operan sobre el periodo completo (--desde/--hasta).

Pasos adicionales: ingesta_vra, ingesta_metar, silver_vuelos, silver_metar, gold_features, modelo_base,
modelo_final, gold_dimensional, gold_kpi, deriva, valor, catalogar, stream, demo, cadena_gold, todo.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

from latam_ops.comun.config import cargar_yaml, lake
from latam_ops.comun.log import obtener_logger

_log = obtener_logger("ejecutar_paso")
_RE_VRA = re.compile(r"VRA_(\d{4})(\d{1,2})\.csv$")


def mes_de_objeto(objeto: str) -> str:
    """'s3://b/bronze/landing/anac_vra/VRA_20261.csv' o 'bronze/landing/.../VRA_20261.csv' -> '2026-01'."""
    m = _RE_VRA.search(objeto or "")
    if not m:
        raise ValueError(f"El objeto no tiene el nombre VRA_AAAAM.csv: {objeto}")
    return f"{m.group(1)}-{int(m.group(2)):02d}"


def _clave(objeto: str) -> str:
    return re.sub(r"^s3a?://[^/]+/", "", objeto)


def _bajar_objeto(objeto: str) -> Path:
    """Descarga el objeto aterrizado (S3 o disco local) a un temporal."""
    lk = lake()
    tmp = Path(tempfile.mkstemp(prefix="landing_")[1])
    if lk.es_s3:
        import boto3

        boto3.client("s3").download_file(lk.base_boto, _clave(objeto), str(tmp))
    else:
        origen = Path(objeto) if Path(objeto).exists() else Path(lk.base_boto) / _clave(objeto)
        tmp.write_bytes(origen.read_bytes())
    return tmp


def catalogar() -> None:
    """Lanza el crawler de Glue sobre gold."""
    import boto3

    nombre = os.environ.get("LATAM_CRAWLER") or os.environ.get("CRAWLER_NAME", "latam-ops-gold")
    glue = boto3.client("glue", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    try:
        glue.start_crawler(Name=nombre)
        _log.info(f"crawler {nombre} iniciado")
    except glue.exceptions.CrawlerRunningException:
        _log.info(f"crawler {nombre} ya estaba en ejecución")


def ejecutar_cadena_gold(desde: str, hasta: str) -> None:
    """features -> predicciones (si hay modelo activo) -> estrella -> KPI."""
    from latam_ops.transformacion import gold_dimensional, gold_features, gold_kpi

    gold_features.ejecutar(desde, hasta)
    _predecir_si_hay_modelo(desde, hasta)
    gold_dimensional.ejecutar(desde, hasta)
    gold_kpi.ejecutar(desde, hasta)


def _predecir_si_hay_modelo(desde: str, hasta: str) -> None:
    from latam_ops.ml import predecir

    try:
        predecir.ejecutar([], desde, hasta)
    except FileNotFoundError as exc:
        _log.warning(f"sin modelo activo, se omiten predicciones: {exc}")


def ejecutar(paso: str, desde: str, hasta: str, origen: str = "http", dir_raw: str | None = None,
             objeto: str | None = None) -> None:
    """Despacha el paso pedido."""
    from latam_ops.conectores import metar, vra

    if objeto:
        desde = hasta = mes_de_objeto(objeto)
    extra = ["--origen", origen] + (["--dir-raw", dir_raw] if dir_raw else [])

    if paso == "validar":
        if not objeto:
            _log.info("sin objeto: nada que validar")
            return
        archivo = _bajar_objeto(objeto)
        vra.validar_vra(archivo)
        _log.info(f"objeto válido {objeto} -> mes {desde}")
    elif paso == "ingerir":
        if objeto:
            a, m = map(int, desde.split("-"))
            con = vra.ConectorVra(lake())
            destino = f"{lake().relativa('bronze_vra')}/year={a}/month={m:02d}/VRA_{a}{m}.csv"
            con.subir(_bajar_objeto(objeto), destino, objeto, vra.validar_vra)
            # El METAR es anual: si el año ya está en bronze no se vuelve a pedir a IEM.
            from latam_ops.comun.almacen import Almacen

            prefijo_metar = f"{lake().relativa('bronze_metar')}/year={a}/"
            if Almacen(lake()).listar(prefijo_metar):
                _log.info(f"METAR {a} ya está en bronze, no se descarga de nuevo")
            else:
                metar.main(["--desde", desde, "--hasta", hasta, "--origen", "http"])
        else:
            vra.main(["--desde", desde, "--hasta", hasta, *extra])
            metar.main(["--desde", desde, "--hasta", hasta, *extra])
    elif paso == "ingesta_vra":
        vra.main(["--desde", desde, "--hasta", hasta, *extra])
    elif paso == "ingesta_metar":
        metar.main(["--desde", desde, "--hasta", hasta, *extra])
    elif paso == "silver_vuelos":
        from latam_ops.transformacion import silver_vuelos
        silver_vuelos.ejecutar(desde, hasta)
    elif paso == "silver_metar":
        from latam_ops.transformacion import silver_metar
        silver_metar.ejecutar(desde, hasta)
    elif paso == "silver":
        ejecutar("silver_vuelos", desde, hasta)
        ejecutar("silver_metar", desde, hasta)
    elif paso in ("gold", "gold_features"):
        from latam_ops.transformacion import gold_features
        gold_features.ejecutar(desde, hasta)
    elif paso == "predecir":
        _predecir_si_hay_modelo(desde, hasta)
    elif paso == "kpi":
        ejecutar("gold_dimensional", desde, hasta)
        ejecutar("gold_kpi", desde, hasta)
    elif paso == "gold_dimensional":
        from latam_ops.transformacion import gold_dimensional
        gold_dimensional.ejecutar(desde, hasta)
    elif paso == "gold_kpi":
        from latam_ops.transformacion import gold_kpi
        gold_kpi.ejecutar(desde, hasta)
    elif paso == "cadena_gold":
        ejecutar_cadena_gold(desde, hasta)
    elif paso == "modelo_base":
        from latam_ops.ml import modelo_base
        modelo_base.ejecutar()
    elif paso == "modelo_final":
        from latam_ops.ml import modelo_final
        modelo_final.ejecutar(float(os.environ.get("LATAM_FRACCION_TRAIN", "1.0")))
    elif paso == "deriva":
        from latam_ops.ml import deriva
        deriva.ejecutar()
    elif paso == "valor":
        from latam_ops.ml import valor_negocio
        valor_negocio.ejecutar()
    elif paso == "catalogar":
        catalogar()
    elif paso == "stream":
        from latam_ops.streaming import landing_stream
        landing_stream.ejecutar()
    elif paso == "demo":
        # El archivo del mes ya fue dejado en bronze/landing por scripts/demo_subir_mes.sh.
        from latam_ops.streaming import landing_stream
        meses = landing_stream.ejecutar(continuar_gold=False) or [desde]
        for mes in meses:
            ejecutar("ingesta_metar", mes, mes, origen, dir_raw)
            ejecutar("silver_metar", mes, mes)
            ejecutar_cadena_gold(mes, mes)
        if os.environ.get("LATAM_MODO") == "aws":
            catalogar()
    elif paso == "todo":
        for p in ("ingesta_vra", "ingesta_metar", "silver_vuelos", "silver_metar", "gold_features", "modelo_base",
                  "modelo_final"):
            ejecutar(p, desde, hasta, origen, dir_raw)
        demo = cargar_yaml("pipeline")["periodo"]["demo"]
        for p in ("predecir", "kpi"):
            ejecutar(p, desde, hasta, origen, dir_raw)
        ejecutar("deriva", desde, hasta)
        ejecutar("valor", desde, hasta)
        _log.info(f"el mes {demo} queda reservado para la demo (make demo)")
    else:
        raise SystemExit(f"Paso desconocido: {paso}")


def main(argv: list[str] | None = None) -> None:
    per = cargar_yaml("pipeline")["periodo"]
    p = argparse.ArgumentParser(description="Ejecuta un paso del pipeline")
    p.add_argument("paso")
    p.add_argument("objeto", nargs="?", default=None, help="objeto aterrizado (s3://.../VRA_AAAAM.csv), opcional")
    p.add_argument("--objeto", dest="objeto_opt", default=None)
    p.add_argument("--desde", default=per["inicio"])
    p.add_argument("--hasta", default=per["fin"])
    p.add_argument("--origen", default=os.environ.get("LATAM_ORIGEN", "http"), choices=["http", "local"])
    p.add_argument("--dir-raw", default=None)
    a = p.parse_args(argv)
    objeto = a.objeto_opt or a.objeto
    _log.info(f"inicio paso {a.paso} objeto={objeto} {a.desde}..{a.hasta}")
    ejecutar(a.paso, a.desde, a.hasta, a.origen, a.dir_raw, objeto)
    _log.info(f"fin paso {a.paso}")


if __name__ == "__main__":
    main(sys.argv[1:])
