#!/usr/bin/env bash
# Costo del mes en curso: total de la cuenta y por servicio, y lo etiquetado Project=<proyecto>.
# Nota: el filtro por etiqueta solo funciona despues de activar "Project" como cost allocation tag
# en Billing (tarda hasta 24 h en aparecer). Cost Explorer cobra 0,01 USD por llamada a la API.
set -euo pipefail
source "$(dirname "$0")/lib_infra.sh"
requiere aws jq

INICIO="$(date -u +%Y-%m-01)"
FIN="$(date -u -d tomorrow +%Y-%m-%d)"
[[ "$INICIO" < "$FIN" ]] || FIN="$(date -u -d "$INICIO +1 day" +%Y-%m-%d)"

info "periodo $INICIO .. $FIN (USD, costo no combinado)"

aws ce get-cost-and-usage --region us-east-1 \
  --time-period "Start=$INICIO,End=$FIN" --granularity MONTHLY --metrics UnblendedCost \
  --group-by Type=DIMENSION,Key=SERVICE \
  | jq -r '.ResultsByTime[].Groups[] | [.Keys[0], (.Metrics.UnblendedCost.Amount|tonumber|.*100|round/100)] | @tsv' \
  | sort -t$'\t' -k2 -nr | awk -F'\t' '$2 > 0 {printf "  %-45s %8.2f\n", $1, $2; t+=$2} END {printf "  %-45s %8.2f\n", "TOTAL CUENTA", t}'

echo
info "solo recursos con Project=$PROYECTO"
aws ce get-cost-and-usage --region us-east-1 \
  --time-period "Start=$INICIO,End=$FIN" --granularity MONTHLY --metrics UnblendedCost \
  --filter "{\"Tags\":{\"Key\":\"Project\",\"Values\":[\"$PROYECTO\"]}}" \
  | jq -r '.ResultsByTime[] | "  total etiquetado: " + (.Total.UnblendedCost.Amount|tonumber|.*100|round/100|tostring) + " USD"'
