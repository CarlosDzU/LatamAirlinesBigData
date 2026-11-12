# Security group del cluster: ningun ingreso desde internet (acceso humano solo por SSM).
# Ingreso: unicamente desde el propio SG (master <-> workers: 7077, puertos de executors, bloques).
# Egreso: 443 a internet (dnf de AL2023, archive.apache.org, Maven Central, PyPI, ANAC, IEM,
# APIs de AWS) y todo el trafico dentro del SG. No se abre el 80: todos los origenes usan HTTPS.

resource "aws_security_group" "cluster" {
  name        = "${var.proyecto}-cluster"
  description = "Nodos Spark del proyecto: sin ingreso desde internet"
  vpc_id      = aws_vpc.cluster.id
  tags        = { Name = "${var.proyecto}-cluster" }
}

resource "aws_vpc_security_group_ingress_rule" "entre_nodos" {
  security_group_id            = aws_security_group.cluster.id
  description                  = "Todo el trafico entre nodos del cluster"
  ip_protocol                  = "-1"
  referenced_security_group_id = aws_security_group.cluster.id
}

resource "aws_vpc_security_group_egress_rule" "https" {
  security_group_id = aws_security_group.cluster.id
  description       = "HTTPS saliente (paquetes, fuentes de datos, APIs AWS)"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "entre_nodos" {
  security_group_id            = aws_security_group.cluster.id
  description                  = "Todo el trafico hacia otros nodos del cluster"
  ip_protocol                  = "-1"
  referenced_security_group_id = aws_security_group.cluster.id
}
