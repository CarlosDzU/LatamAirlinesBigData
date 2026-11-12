#!/usr/bin/env bash
# Publica el codigo versionado en s3://<bucket>/scripts/ y, si el master esta encendido,
# lo actualiza en /opt/proyecto de todos los nodos del proyecto (SSM Run Command).
set -euo pipefail
source "$(dirname "$0")/lib_infra.sh"
requiere aws terraform git

BUCKET="${LAKE_BUCKET:-$(salida_base bucket_name)}"
DESTINO="s3://$BUCKET/scripts"
cd "$RAIZ"

VERSION="$(git rev-parse --short HEAD 2>/dev/null || echo sin-commit)$(git diff --quiet 2>/dev/null || echo -sucio)"
info "publicando version $VERSION en $DESTINO"

for carpeta in src config contratos scripts notebooks; do
  [[ -d "$carpeta" ]] || continue
  aws s3 sync "$carpeta" "$DESTINO/$carpeta" --delete --only-show-errors \
    --exclude '*__pycache__*' --exclude '*.pyc' --exclude '.ipynb_checkpoints/*'
done
for archivo in requirements.txt pyproject.toml; do
  [[ -f "$archivo" ]] && aws s3 cp "$archivo" "$DESTINO/$archivo" --only-show-errors
done
printf '%s\n' "$VERSION" | aws s3 cp - "$DESTINO/VERSION" --only-show-errors

# Actualizacion en caliente de los nodos encendidos (si los hay)
IDS=$(aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=$PROYECTO" "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].InstanceId' --output text)
if [[ -z "$IDS" ]]; then
  info "no hay nodos encendidos; el codigo se instalara en el proximo make compute-up"
  exit 0
fi

COMANDO="aws s3 sync s3://$BUCKET/scripts/ /opt/proyecto/ --delete --exclude 'docs/evidencias/*' --only-show-errors && chown -R ec2-user:ec2-user /opt/proyecto && find /opt/proyecto/scripts -name '*.sh' -exec chmod 755 {} + ; if [ -f /opt/proyecto/requirements.txt ]; then python3.11 -m pip install --quiet -r /opt/proyecto/requirements.txt; fi; if [ -f /opt/proyecto/pyproject.toml ]; then python3.11 -m pip install --quiet -e /opt/proyecto; fi"
# shellcheck disable=SC2086
CMD_ID=$(aws ssm send-command --instance-ids $IDS --document-name AWS-RunShellScript \
  --comment "sync codigo $VERSION" \
  --parameters "$(jq -cn --arg c "$COMANDO" '{commands: [$c]}')" \
  --query 'Command.CommandId' --output text)
info "actualizando nodos ($IDS), comando $CMD_ID"
for id in $IDS; do
  aws ssm wait command-executed --command-id "$CMD_ID" --instance-id "$id" \
    || error "fallo la actualizacion en $id (ver: aws ssm get-command-invocation --command-id $CMD_ID --instance-id $id)"
done
info "codigo $VERSION instalado en todos los nodos"
