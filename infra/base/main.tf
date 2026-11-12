provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project = var.proyecto
      Owner   = var.owner
      Env     = var.entorno
      Stack   = "base"
    }
  }
}

data "aws_caller_identity" "actual" {}
data "aws_partition" "actual" {}

resource "random_id" "sufijo" {
  byte_length = 4
}

locals {
  cuenta        = data.aws_caller_identity.actual.account_id
  particion     = data.aws_partition.actual.partition
  bucket_nombre = "${var.proyecto}-lake-${random_id.sufijo.hex}"
  glue_db       = replace(var.proyecto, "-", "_")
}
