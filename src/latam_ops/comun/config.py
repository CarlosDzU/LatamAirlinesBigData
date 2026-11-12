"""Carga de configuración y resolución de rutas del lake (local o S3)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import yaml

RAIZ_REPO = Path(__file__).resolve().parents[3]


def ruta_repo(*partes: str) -> Path:
    """Devuelve una ruta absoluta dentro del repositorio (o de /opt/proyecto en el clúster)."""
    base = Path(os.environ.get("LATAM_RAIZ", RAIZ_REPO))
    return base.joinpath(*partes)


@cache
def cargar_yaml(nombre: str) -> dict[str, Any]:
    """Lee un YAML de config/ por nombre (sin extensión)."""
    with open(ruta_repo("config", f"{nombre}.yaml"), encoding="utf-8") as f:
        return yaml.safe_load(f)


def modo() -> str:
    """'local' (Spark local[*] sobre data/lake) o 'aws' (clúster sobre s3a://LAKE_BUCKET)."""
    valor = os.environ.get("LATAM_MODO", "local").lower()
    if valor not in {"local", "aws"}:
        raise ValueError(f"LATAM_MODO inválido: {valor}")
    return valor


@dataclass(frozen=True)
class Lake:
    """Resuelve rutas lógicas del lake (p. ej. 'silver_vuelos') a rutas físicas."""

    base: str          # file:///.../data/lake  o  s3a://bucket
    base_boto: str     # ruta local absoluta o 'bucket' (para boto3)
    es_s3: bool

    def ruta(self, clave_o_relativa: str) -> str:
        """Ruta Spark de una tabla declarada en pipeline.yaml (clave) o de una ruta relativa."""
        tablas = cargar_yaml("pipeline")["tablas"]
        rel = tablas.get(clave_o_relativa, clave_o_relativa)
        return f"{self.base}/{rel}"

    def relativa(self, clave_o_relativa: str) -> str:
        """Ruta relativa (prefijo S3 o subcarpeta local) de una tabla."""
        tablas = cargar_yaml("pipeline")["tablas"]
        return tablas.get(clave_o_relativa, clave_o_relativa)


def lake(raiz_local: str | None = None) -> Lake:
    """Construye el resolvedor de rutas según el modo de ejecución."""
    cfg = cargar_yaml("pipeline")["lake"]
    if modo() == "aws":
        bucket = os.environ.get(cfg["bucket_env"], "").strip()
        if not bucket:
            raise RuntimeError(f"Falta la variable de entorno {cfg['bucket_env']} con el nombre del bucket")
        return Lake(base=f"s3a://{bucket}", base_boto=bucket, es_s3=True)
    raiz = Path(raiz_local or os.environ.get("LATAM_LAKE_LOCAL", ruta_repo(cfg["raiz_local"]))).resolve()
    raiz.mkdir(parents=True, exist_ok=True)
    return Lake(base=raiz.as_uri(), base_boto=str(raiz), es_s3=False)


def empresas_latam() -> list[str]:
    """Códigos ICAO de empresa del grupo LATAM (config/aerolineas_latam.yaml o respaldo en fuentes.yaml)."""
    archivo = ruta_repo("config", "aerolineas_latam.yaml")
    if archivo.exists():
        with open(archivo, encoding="utf-8") as f:
            datos = yaml.safe_load(f) or {}
        codigos = _extraer_codigos(datos)
        if codigos:
            return sorted(set(codigos))
    return list(cargar_yaml("fuentes")["anac_vra"]["empresas_latam_respaldo"])


def _extraer_codigos(datos: Any) -> list[str]:
    """Extrae códigos ICAO de 3 letras de un YAML con estructura flexible."""
    codigos: list[str] = []
    if isinstance(datos, dict):
        for clave in ("empresas_latam", "codigos_icao", "icao", "empresas"):
            if clave in datos:
                return _extraer_codigos(datos[clave])
        for v in datos.values():
            codigos += _extraer_codigos(v)
    elif isinstance(datos, list):
        for item in datos:
            if isinstance(item, str) and len(item) == 3 and item.isalpha():
                codigos.append(item.upper())
            elif isinstance(item, dict):
                valor = item.get("icao") or item.get("codigo_icao") or item.get("codigo")
                incluir = item.get("incluir", item.get("activa", True))
                if isinstance(valor, str) and incluir:
                    codigos.append(valor.upper())
    return codigos


def periodo_meses(desde: str, hasta: str) -> list[tuple[int, int]]:
    """Lista de (año, mes) entre dos 'AAAA-MM' inclusive."""
    a1, m1 = map(int, desde.split("-"))
    a2, m2 = map(int, hasta.split("-"))
    salida = []
    a, m = a1, m1
    while (a, m) <= (a2, m2):
        salida.append((a, m))
        m += 1
        if m == 13:
            a, m = a + 1, 1
    return salida
