#!/usr/bin/env bash
# Ejecuta un comando en el master via SSM Run Command, espera y muestra la salida.
# Uso: scripts/ejecutar_remoto.sh "<comando>"
#   ej.: scripts/ejecutar_remoto.sh "/opt/proyecto/scripts/ejecutar_paso.sh silver"
# El comando corre como ec2-user con el entorno del proyecto (/etc/profile.d/latam_ops.sh).
set -euo pipefail
source "$(dirname "$0")/lib_infra.sh"
requiere aws jq

[[ $# -ge 1 ]] || error "uso: $0 \"<comando>\""
COMANDO="$*"
ID="$(master_id)"
GRUPO_LOGS="/$PROYECTO/ssm"

CMD_ID=$(aws ssm send-command --instance-ids "$ID" --document-name AWS-RunShellScript \
  --comment "remoto: ${COMANDO:0:80}" \
  --cloud-watch-output-config "CloudWatchOutputEnabled=true,CloudWatchLogGroupName=$GRUPO_LOGS" \
  --parameters "$(jq -cn --arg c "sudo -u ec2-user -H bash -lc $(printf '%q' "$COMANDO")" '{commands: [$c], executionTimeout: ["7200"]}')" \
  --query 'Command.CommandId' --output text)
info "comando $CMD_ID enviado a $ID; esperando..."

while true; do
  ESTADO=$(aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$ID" \
    --query Status --output text 2>/dev/null || echo Pending)
  case "$ESTADO" in
    Pending|InProgress|Delayed) sleep 10 ;;
    *) break ;;
  esac
done

aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$ID" \
  --query '[StandardOutputContent, StandardErrorContent]' --output text
info "estado final: $ESTADO (salida completa en CloudWatch: $GRUPO_LOGS)"
[[ "$ESTADO" == "Success" ]]
