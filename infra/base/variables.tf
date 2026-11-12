variable "region" {
  description = "Region de AWS del proyecto."
  type        = string
  default     = "us-east-1"
  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9]$", var.region))
    error_message = "Region invalida (ej. us-east-1)."
  }
}

variable "proyecto" {
  description = "Prefijo de nombres y valor de la etiqueta Project."
  type        = string
  default     = "latam-ops"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.proyecto))
    error_message = "Usar minusculas, numeros y guiones (3-21 caracteres)."
  }
}

variable "entorno" {
  description = "Entorno (etiqueta Env)."
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "prod"], var.entorno)
    error_message = "entorno debe ser dev o prod."
  }
}

variable "owner" {
  description = "Responsable del proyecto (etiqueta Owner)."
  type        = string
  validation {
    condition     = length(var.owner) > 1
    error_message = "owner es obligatorio."
  }
}

variable "force_destroy" {
  description = "Permite destruir el bucket con objetos dentro. Solo true al cierre del curso."
  type        = bool
  default     = false
}

variable "budget_usd" {
  description = "Presupuesto mensual en USD para las alertas de AWS Budgets."
  type        = number
  default     = 100
  validation {
    condition     = var.budget_usd > 0 && var.budget_usd <= 1000
    error_message = "budget_usd debe estar entre 1 y 1000."
  }
}

variable "alert_emails" {
  description = "Correos que reciben alertas de presupuesto y del pipeline (definir en terraform.tfvars, fuera de git)."
  type        = list(string)
  validation {
    condition     = length(var.alert_emails) > 0 && alltrue([for e in var.alert_emails : can(regex("^[^@ ]+@[^@ ]+\\.[^@ ]+$", e))])
    error_message = "Indicar al menos un correo valido."
  }
}

variable "athena_limite_bytes" {
  description = "Tope de bytes escaneados por consulta en el workgroup de Athena."
  type        = number
  default     = 10737418240 # 10 GB
  validation {
    condition     = var.athena_limite_bytes >= 10485760
    error_message = "El minimo permitido por Athena es 10 MB."
  }
}

variable "retencion_logs_dias" {
  description = "Dias de retencion de los log groups de CloudWatch."
  type        = number
  default     = 14
  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90], var.retencion_logs_dias)
    error_message = "Usar un valor de retencion admitido por CloudWatch (1-90)."
  }
}

variable "crear_oidc_github" {
  description = "Crea el proveedor OIDC de GitHub y el rol de solo lectura para terraform plan en CI."
  type        = bool
  default     = true
}

variable "github_repo" {
  description = "Repositorio de GitHub (owner/nombre) autorizado a asumir el rol de CI."
  type        = string
  default     = "CarlosDzU/LatamAirlinesBigData"
  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repo))
    error_message = "Formato owner/repositorio."
  }
}

variable "ruta_salt" {
  description = "Nombre del parametro SSM con la salt de seudonimizacion (lo crea scripts/crear_salt.sh, no Terraform)."
  type        = string
  default     = "/latam-ops/hash-salt"
}

variable "pasos_pipeline" {
  description = "Pasos que la maquina de estados ejecuta en orden en el master (argumento de scripts/ejecutar_paso.sh)."
  type        = list(string)
  default     = ["validar", "ingerir", "silver", "gold", "predecir", "kpi", "valor"]
}
