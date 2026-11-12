#!/usr/bin/env bash
# Abre un tunel SSM (sin SSH ni puertos abiertos) desde localhost:<local> al puerto <remoto> del master.
# Uso: scripts/tunel_ssm.sh [puerto_local] [puerto_remoto]
#   8888 Jupyter | 8080 Spark master UI | 4040 UI de la aplicacion | 18080 History Server
# Requiere el Session Manager plugin instalado en la maquina local.
set -euo pipefail
source "$(dirname "$0")/lib_infra.sh"
requiere aws session-manager-plugin

LOCAL="${1:-8888}"
REMOTO="${2:-$LOCAL}"
ID="$(master_id)"

info "tunel localhost:$LOCAL -> $ID:$REMOTO (Ctrl+C para cerrar)"
exec aws ssm start-session --target "$ID" \
  --document-name AWS-StartPortForwardingSession \
  --parameters "portNumber=$REMOTO,localPortNumber=$LOCAL"
