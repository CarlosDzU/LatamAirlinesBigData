# Red minima sin NAT: una subred publica (salida a internet por IGW solo para paquetes y fuentes
# HTTPS) y un endpoint gateway de S3 (gratis) para que el trafico al lake no salga a internet.

resource "aws_vpc" "cluster" {
  #checkov:skip=CKV2_AWS_11:Flow logs de VPC omitidos por costo; el cluster es efimero y sin ingreso desde internet.
  cidr_block           = "10.20.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${var.proyecto}-vpc" }
}

# Cierra el SG por defecto de la VPC (sin reglas).
resource "aws_default_security_group" "cerrado" {
  vpc_id = aws_vpc.cluster.id
}

resource "aws_internet_gateway" "cluster" {
  vpc_id = aws_vpc.cluster.id
  tags   = { Name = "${var.proyecto}-igw" }
}

resource "aws_subnet" "publica" {
  #checkov:skip=CKV_AWS_130:IP publica necesaria para salir a internet sin NAT (prohibido por costo); el SG no admite ingreso desde internet.
  vpc_id                  = aws_vpc.cluster.id
  cidr_block              = "10.20.1.0/24"
  availability_zone       = local.az
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.proyecto}-subred-publica" }
}

resource "aws_route_table" "publica" {
  vpc_id = aws_vpc.cluster.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.cluster.id
  }
  tags = { Name = "${var.proyecto}-rutas" }
}

resource "aws_route_table_association" "publica" {
  subnet_id      = aws_subnet.publica.id
  route_table_id = aws_route_table.publica.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.cluster.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.publica.id]
  tags              = { Name = "${var.proyecto}-endpoint-s3" }
}
