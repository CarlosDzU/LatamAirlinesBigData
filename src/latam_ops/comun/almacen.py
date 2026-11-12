"""Acceso a archivos del lake fuera de Spark (ingesta, manifiestos): disco local o S3 vía boto3."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from latam_ops.comun.config import Lake


class Almacen:
    """Operaciones mínimas sobre el lake: existe, escribir, leer, listar."""

    def __init__(self, lake: Lake) -> None:
        self.lake = lake
        self._s3 = None
        if lake.es_s3:
            import boto3

            self._s3 = boto3.client("s3")

    def _local(self, rel: str) -> Path:
        return Path(self.lake.base_boto) / rel

    def existe(self, rel: str) -> bool:
        """True si el objeto/archivo existe."""
        if self._s3:
            try:
                self._s3.head_object(Bucket=self.lake.base_boto, Key=rel)
                return True
            except self._s3.exceptions.ClientError:
                return False
        return self._local(rel).exists()

    def escribir_archivo(self, rel: str, origen: Path) -> None:
        """Copia un archivo local al lake."""
        if self._s3:
            self._s3.upload_file(str(origen), self.lake.base_boto, rel)
            return
        destino = self._local(rel)
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_bytes(origen.read_bytes())

    def escribir_json(self, rel: str, datos: Any) -> None:
        """Escribe un JSON (manifiestos, reportes)."""
        cuerpo = json.dumps(datos, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        if self._s3:
            self._s3.put_object(Bucket=self.lake.base_boto, Key=rel, Body=cuerpo, ContentType="application/json")
            return
        destino = self._local(rel)
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_bytes(cuerpo)

    def leer_json(self, rel: str) -> Any | None:
        """Lee un JSON o devuelve None si no existe."""
        if not self.existe(rel):
            return None
        if self._s3:
            cuerpo = self._s3.get_object(Bucket=self.lake.base_boto, Key=rel)["Body"].read()
            return json.loads(cuerpo)
        return json.loads(self._local(rel).read_text(encoding="utf-8"))

    def listar(self, prefijo: str) -> list[str]:
        """Rutas relativas bajo un prefijo."""
        if self._s3:
            paginas = self._s3.get_paginator("list_objects_v2").paginate(Bucket=self.lake.base_boto, Prefix=prefijo)
            return [o["Key"] for p in paginas for o in p.get("Contents", [])]
        base = self._local(prefijo)
        if not base.exists():
            return []
        raiz = Path(self.lake.base_boto)
        return sorted(str(p.relative_to(raiz)) for p in base.rglob("*") if p.is_file())
