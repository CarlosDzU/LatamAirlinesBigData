provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project = var.proyecto
      Owner   = var.owner
      Env     = var.entorno
      Stack   = "compute"
    }
  }
}

data "aws_caller_identity" "actual" {}
data "aws_partition" "actual" {}

data "aws_availability_zones" "disponibles" {
  #checkov:skip=CKV_AWS_394:La AZ se fija con la variable zona_disponibilidad; por defecto se toma la primera disponible
  state = "available"
}

# Amazon Linux 2023 mas reciente (x86_64) publicado por AWS en SSM.
data "aws_ssm_parameter" "ami_al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_s3_bucket" "lake" {
  bucket = var.bucket_name
}

locals {
  cuenta    = data.aws_caller_identity.actual.account_id
  particion = data.aws_partition.actual.partition
  az        = var.zona_disponibilidad != "" ? var.zona_disponibilidad : data.aws_availability_zones.disponibles.names[0]

  agente_cloudwatch = templatefile("${path.module}/templates/cloudwatch-agent.json.tftpl", {
    log_group_jobs  = var.log_group_jobs
    log_group_spark = var.log_group_spark
  })

  # Bloque comun a master y workers (instalacion idempotente).
  bootstrap_comun = templatefile("${path.module}/templates/bootstrap_comun.sh.tftpl", {
    bucket            = var.bucket_name
    region            = var.region
    spark_version     = var.spark_version
    spark_sha512      = var.spark_sha512
    agente_cloudwatch = var.habilitar_agente_cloudwatch
    config_agente     = local.agente_cloudwatch
    proyecto          = var.proyecto
  })

  spark_defaults = templatefile("${path.module}/templates/spark-defaults.conf.tftpl", {
    bucket         = var.bucket_name
    region         = var.region
    driver_mem     = var.memoria.driver
    executor_mem   = var.memoria.executor
    executor_cores = var.memoria.executor_cores
  })
}
