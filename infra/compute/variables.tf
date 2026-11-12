variable "region" {
  description = "Region de AWS (la misma del stack base)."
  type        = string
  default     = "us-east-1"
}

variable "proyecto" {
  description = "Prefijo de nombres y etiqueta Project (igual que en base)."
  type        = string
  default     = "latam-ops"
}

variable "entorno" {
  description = "Entorno (etiqueta Env)."
  type        = string
  default     = "dev"
}

variable "owner" {
  description = "Responsable (etiqueta Owner)."
  type        = string
  default     = "carlos"
}

variable "bucket_name" {
  description = "Bucket del data lake (output bucket_name del stack base). Este stack nunca lo crea ni lo destruye."
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{2,62}$", var.bucket_name))
    error_message = "Nombre de bucket invalido."
  }
}

variable "crawler_name" {
  description = "Crawler de gold (output del stack base)."
  type        = string
}

variable "log_group_jobs" {
  description = "Log group de los jobs (output del stack base)."
  type        = string
}

variable "log_group_ssm" {
  description = "Log group de la salida de SSM Run Command (output del stack base)."
  type        = string
}

variable "log_group_spark" {
  description = "Log group de Spark (output del stack base)."
  type        = string
}

variable "sns_topic_arn" {
  description = "Topico SNS de alertas (output del stack base)."
  type        = string
}

variable "ruta_salt" {
  description = "Parametro SSM con la salt de seudonimizacion."
  type        = string
  default     = "/latam-ops/hash-salt"
}

variable "instance_type" {
  description = "Tipo de instancia para master y workers (solo tipos del plan Free para cuentas nuevas)."
  type        = string
  default     = "m7i-flex.large"
  validation {
    condition     = contains(["t3.micro", "t3.small", "t4g.micro", "t4g.small", "c7i-flex.large", "m7i-flex.large"], var.instance_type)
    error_message = "Tipo no permitido por las reglas de costo del proyecto."
  }
}

variable "worker_count" {
  description = "Numero de workers adicionales (el master tambien ejecuta un worker)."
  type        = number
  default     = 0
  validation {
    condition     = var.worker_count >= 0 && var.worker_count <= 2
    error_message = "worker_count debe estar entre 0 y 2."
  }
}

variable "ebs_gb" {
  description = "Tamano del disco raiz gp3 de cada nodo (GB)."
  type        = number
  default     = 30
  validation {
    condition     = var.ebs_gb >= 20 && var.ebs_gb <= 100
    error_message = "ebs_gb entre 20 y 100."
  }
}

variable "idle_stop_minutes" {
  description = "Minutos con CPU < 5 % tras los cuales CloudWatch detiene la instancia (multiplo de 15)."
  type        = number
  default     = 45
  validation {
    condition     = var.idle_stop_minutes >= 15 && var.idle_stop_minutes % 15 == 0
    error_message = "idle_stop_minutes debe ser multiplo de 15 y >= 15."
  }
}

variable "zona_disponibilidad" {
  description = "AZ de la subred. Vacio = primera AZ de la region."
  type        = string
  default     = ""
}

variable "spark_version" {
  description = "Version de Spark 3.5.x (binario hadoop3 de archive.apache.org)."
  type        = string
  default     = "3.5.9"
  validation {
    condition     = can(regex("^3\\.5\\.[0-9]+$", var.spark_version))
    error_message = "Solo Spark 3.5.x."
  }
}

variable "spark_sha512" {
  description = "SHA-512 publicado del binario spark-<version>-bin-hadoop3.tgz (verificado el 11/11/2026 en archive.apache.org para 3.5.9)."
  type        = string
  default     = "66097faf48b12f8673dc13256a44d731c27c86a5831cd8ca930c6fd9dd9bdf31d40593095e959461351d55da1a8e3ad5ed95e0b4b51bea87ca3989ab002ea8ce"
  validation {
    condition     = can(regex("^[0-9a-f]{128}$", var.spark_sha512))
    error_message = "SHA-512 en hexadecimal minuscula (128 caracteres)."
  }
}

variable "habilitar_agente_cloudwatch" {
  description = "Instala el agente de CloudWatch para enviar logs de jobs y de Spark."
  type        = bool
  default     = true
}

variable "memoria" {
  description = "Memoria de Spark por rol (nodos de 8 GiB)."
  type = object({
    worker_en_master = string
    worker           = string
    driver           = string
    executor         = string
    executor_cores   = number
  })
  default = {
    worker_en_master = "4g"
    worker           = "6g"
    driver           = "2g"
    executor         = "3g"
    executor_cores   = 2
  }
}
