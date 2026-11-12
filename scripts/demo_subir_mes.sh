#!/usr/bin/env bash
# Demo de punta a punta: deja el CSV VRA de un mes nuevo en bronze/landing/anac_vra/.
# En AWS, EventBridge detecta el objeto y Step Functions corre validar -> ingerir -> silver -> gold -> predecir -> kpi
# y cataloga gold; el tablero (Athena) muestra el mes nuevo al refrescar.
# Uso: scripts/demo_subir_mes.sh 2026-01 [local|aws]
set -euo pipefail
MES="${1:?uso: demo_subir_mes.sh AAAA-MM [local|aws]}"
MODO="${2:-${LATAM_MODO:-aws}}"
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ANIO="${MES%-*}"; NUM="${MES#*-}"; M=$((10#$NUM))
NOMBRE="VRA_${ANIO}${M}.csv"
LOCAL="$RAIZ/data/raw/anac_vra/year=${ANIO}/month=${NUM}/${NOMBRE}"
if [[ ! -f "$LOCAL" ]]; then
  echo "[demo] descargando $NOMBRE desde la ANAC"
  mkdir -p "$(dirname "$LOCAL")"
  URL="$(cd "$RAIZ" && PYTHONPATH=src python3 -m latam_ops.conectores.vra --desde "$MES" --hasta "$MES" --dry-run)"
  curl -fsSL -A "latam-ops-academic/1.0" -o "$LOCAL" "$URL"
fi
if [[ "$MODO" == "aws" ]]; then
  BUCKET="${LAKE_BUCKET:?defina LAKE_BUCKET}"
  echo "[demo] subiendo a s3://$BUCKET/bronze/landing/anac_vra/$NOMBRE (dispara Step Functions)"
  aws s3 cp "$LOCAL" "s3://$BUCKET/bronze/landing/anac_vra/$NOMBRE" --only-show-errors
  echo "[demo] seguir la ejecución: aws stepfunctions list-executions --state-machine-arn <arn> --max-items 1"
else
  LAKE="${LATAM_LAKE_LOCAL:-$RAIZ/data/lake}"
  mkdir -p "$LAKE/bronze/landing/anac_vra"
  cp "$LOCAL" "$LAKE/bronze/landing/anac_vra/$NOMBRE"
  echo "[demo] archivo en $LAKE/bronze/landing/anac_vra/ ; procesando con Structured Streaming"
  cd "$RAIZ" && LATAM_MODO=local PYTHONPATH=src python3 -m latam_ops.orquestacion.ejecutar_paso demo \
    --desde "$MES" --hasta "$MES" --origen http
fi
