#!/usr/bin/env bash
# Funciones comunes de los scripts de infraestructura (se incluye con `source`).
# Configuracion: AWS_PROFILE y AWS_REGION desde el entorno o desde .env en la raiz del repo.

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -f "$RAIZ/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$RAIZ/.env"
  set +a
fi
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"
PROYECTO="${PROYECTO:-latam-ops}"

info()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(basename "$0")" "$*" >&2; }
error() { printf '\033[1;31m[%s] ERROR:\033[0m %s\n' "$(basename "$0")" "$*" >&2; exit 1; }

requiere() {
  local cmd
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 || error "falta el comando '$cmd'"
  done
}

# Lee un output del stack base (estado local en infra/base).
salida_base() {
  terraform -chdir="$RAIZ/infra/base" output -raw "$1" 2>/dev/null \
    || error "no se pudo leer el output '$1' de infra/base (¿se ejecuto make base-apply?)"
}

# ID de la instancia master encendida (por etiquetas, no depende del estado de compute).
master_id() {
  local id
  id=$(aws ec2 describe-instances \
    --filters "Name=tag:Project,Values=$PROYECTO" "Name=tag:Rol,Values=master" "Name=instance-state-name,Values=running" \
    --query 'Reservations[0].Instances[0].InstanceId' --output text 2>/dev/null)
  [[ -n "$id" && "$id" != "None" ]] || error "no hay master encendido (make compute-up)"
  printf '%s' "$id"
}
