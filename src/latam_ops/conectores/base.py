"""Base de los conectores de ingesta: descarga robusta, sha256 y manifiesto idempotente."""

from __future__ import annotations

import hashlib
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests

from latam_ops.comun.almacen import Almacen
from latam_ops.comun.config import Lake, cargar_yaml, ruta_repo
from latam_ops.comun.log import obtener_logger, run_id

_log = obtener_logger("conectores")


@dataclass
class ResultadoIngesta:
    """Resultado de ingerir un archivo a bronze."""

    destino: str
    sha256: str
    bytes: int
    filas: int
    estado: str  # nuevo | sin_cambios | faltante


def sha256_archivo(ruta: Path) -> str:
    """sha256 en streaming (no carga el archivo completo en memoria)."""
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            h.update(bloque)
    return h.hexdigest()


def contar_lineas(ruta: Path) -> int:
    """Número de líneas del archivo."""
    with open(ruta, "rb") as f:
        return sum(bloque.count(b"\n") for bloque in iter(lambda: f.read(1 << 20), b""))


def descargar(url: str, destino: Path, params: dict | None = None, reintentos: int = 5, timeout: int = 120) -> bool:
    """Descarga en streaming con reintentos exponenciales. Devuelve False si el recurso no existe (404)."""
    cabeceras = {"User-Agent": cargar_yaml("fuentes")["user_agent"]}
    for intento in range(1, reintentos + 1):
        try:
            with requests.get(url, params=params, headers=cabeceras, stream=True, timeout=timeout) as r:
                if r.status_code == 404:
                    return False
                if r.status_code == 429:
                    raise requests.HTTPError("429 demasiadas peticiones")
                r.raise_for_status()
                with open(destino, "wb") as f:
                    for bloque in r.iter_content(1 << 20):
                        f.write(bloque)
            return True
        except (requests.RequestException, OSError) as exc:
            espera = min(2 ** intento * 5, 300)
            _log.warning(f"descarga fallida ({intento}/{reintentos}) {url}: {exc}; reintento en {espera}s")
            time.sleep(espera)
    raise RuntimeError(f"No se pudo descargar {url}")


class ConectorBase:
    """Ingesta idempotente: si el destino ya tiene un manifiesto con el mismo sha256, no se vuelve a subir."""

    fuente: str = "base"

    def __init__(self, lake: Lake, origen: str = "http", dir_raw: Path | None = None) -> None:
        if origen not in {"http", "local"}:
            raise ValueError("origen debe ser 'http' o 'local'")
        self.lake = lake
        self.origen = origen
        self.almacen = Almacen(lake)
        self.dir_raw = Path(dir_raw) if dir_raw else ruta_repo("data", "raw")

    def rel_manifiesto(self, rel_destino: str) -> str:
        """Manifiesto por archivo: bronze/_manifests/<fuente>/<ruta del archivo>.json."""
        relativo = rel_destino.split("/", 2)[-1] if rel_destino.startswith("bronze/") else rel_destino
        return f"{self.lake.relativa('manifiestos')}/{self.fuente}/{relativo}.json"

    def subir(self, archivo: Path, rel_destino: str, url: str, validar) -> ResultadoIngesta:
        """Valida, calcula sha256 y sube a bronze si cambió. Escribe el manifiesto."""
        validar(archivo)
        digest = sha256_archivo(archivo)
        rel_man = self.rel_manifiesto(rel_destino)
        previo = self.almacen.leer_json(rel_man)
        tam = archivo.stat().st_size
        filas = contar_lineas(archivo)
        if previo and previo.get("sha256") == digest and self.almacen.existe(rel_destino):
            _log.info(f"sin cambios (mismo sha256): {rel_destino}")
            return ResultadoIngesta(rel_destino, digest, tam, filas, "sin_cambios")
        self.almacen.escribir_archivo(rel_destino, archivo)
        self.almacen.escribir_json(rel_man, {
            "fuente": self.fuente, "archivo": rel_destino, "url": url, "bytes": tam, "filas": filas,
            "sha256": digest, "ingestado_utc": datetime.now(UTC).isoformat(timespec="seconds"), "run_id": run_id(),
        })
        _log.info(f"ingestado {rel_destino} ({tam} bytes, {filas} líneas)")
        return ResultadoIngesta(rel_destino, digest, tam, filas, "nuevo")

    def obtener(self, url: str, rel_raw: str, params: dict | None = None) -> Path | None:
        """Devuelve el archivo a ingerir: desde data/raw (origen local) o descargándolo (origen http)."""
        if self.origen == "local":
            ruta = self.dir_raw / rel_raw
            return ruta if ruta.exists() else None
        tmp = Path(tempfile.mkstemp(prefix=f"{self.fuente}_")[1])
        return tmp if descargar(url, tmp, params=params) else None
