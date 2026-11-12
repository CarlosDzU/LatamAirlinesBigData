#!/usr/bin/env bash
# Crea (si no existe) la salt de seudonimizacion como SecureString en SSM Parameter Store.
# No se crea con Terraform para que el valor nunca quede en el estado. El valor jamas se imprime.
set -euo pipefail
source "$(dirname "$0")/lib_infra.sh"
requiere aws openssl

RUTA="${RUTA_SALT:-/$PROYECTO/hash-salt}"

if aws ssm get-parameter --name "$RUTA" --query 'Parameter.Name' --output text >/dev/null 2>&1; then
  info "la salt ya existe en $RUTA (no se modifica)"
  exit 0
fi

aws ssm put-parameter \
  --name "$RUTA" \
  --type SecureString \
  --description "Salt de seudonimizacion del proyecto $PROYECTO" \
  --value "$(openssl rand -hex 32)" \
  --tags "Key=Project,Value=$PROYECTO" >/dev/null
info "salt creada en $RUTA (SecureString, clave administrada aws/ssm)"
