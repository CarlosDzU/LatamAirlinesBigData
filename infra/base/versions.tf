terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.80, < 7.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.6"
    }
  }
  # Estado LOCAL a proposito: este stack crea el bucket donde vive el estado de compute.
  # El archivo terraform.tfstate queda fuera de git (infra/.gitignore).
}
