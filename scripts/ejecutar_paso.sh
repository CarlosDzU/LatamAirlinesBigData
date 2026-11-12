#!/usr/bin/env bash
# Ejecuta un paso del pipeline. En el clúster lo invoca Step Functions vía SSM:
#   /opt/proyecto/scripts/ejecutar_paso.sh <paso> [s3://bucket/bronze/landing/anac_vra/VRA_AAAAM.csv]
# En local:  LATAM_MODO=local scripts/ejecutar_paso.sh silver --desde 2024-01 --hasta 2024-01
set -euo pipefail
PASO="${1:?uso: ejecutar_paso.sh <paso> [objeto_s3] [--desde AAAA-MM --hasta AAAA-MM]}"
shift
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -f /etc/profile.d/latam_ops.sh ]]; then set +u; . /etc/profile.d/latam_ops.sh; set -u; fi
export LATAM_MODO="${LATAM_MODO:-aws}"
export LATAM_RAIZ="$RAIZ"
export PYTHONPATH="$RAIZ/src${PYTHONPATH:+:$PYTHONPATH}"
cd "$RAIZ"

if [[ "$LATAM_MODO" == "aws" ]] && command -v spark-submit >/dev/null 2>&1; then
  ZIP="$(mktemp -d)/latam_ops.zip"
  (cd "$RAIZ/src" && python3.11 -m zipfile -c "$ZIP" latam_ops >/dev/null)
  MASTER_ARG=()
  [[ -n "${SPARK_MASTER_URL:-}" ]] && MASTER_ARG=(--master "$SPARK_MASTER_URL")
  exec spark-submit "${MASTER_ARG[@]}" --py-files "$ZIP" \
    --conf "spark.executorEnv.LATAM_RAIZ=$RAIZ" --conf "spark.executorEnv.PYTHONPATH=$RAIZ/src" \
    "$RAIZ/scripts/lanzador.py" "$PASO" "$@"
fi
exec "${PYTHON:-python3}" -m latam_ops.orquestacion.ejecutar_paso "$PASO" "$@"
