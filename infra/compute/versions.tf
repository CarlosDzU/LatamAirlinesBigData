terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.80, < 7.0"
    }
  }

  # Backend parcial: bucket, key y region se pasan con -backend-config desde infra/infra.mk
  # (nunca se escribe aqui el nombre del bucket). use_lockfile evita DynamoDB.
  backend "s3" {}
}
