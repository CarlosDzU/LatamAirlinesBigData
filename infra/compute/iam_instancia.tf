# Rol de las instancias: minimo privilegio. Sin llaves estaticas (credenciales temporales por IMDSv2).

data "aws_iam_policy_document" "ec2_confianza" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "nodo" {
  name               = "${var.proyecto}-nodo"
  assume_role_policy = data.aws_iam_policy_document.ec2_confianza.json
}

data "aws_iam_policy_document" "nodo" {
  statement {
    sid       = "ListarLake"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [data.aws_s3_bucket.lake.arn]
  }
  statement {
    sid       = "ObjetosLake"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${data.aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid       = "LeerSalt"
    actions   = ["ssm:GetParameter"]
    resources = ["arn:${local.particion}:ssm:${var.region}:${local.cuenta}:parameter${var.ruta_salt}"]
  }
  statement {
    sid       = "DescifrarSaltConClaveAdministradaSsm"
    actions   = ["kms:Decrypt"]
    resources = ["arn:${local.particion}:kms:${var.region}:${local.cuenta}:key/*"]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.${var.region}.amazonaws.com"]
    }
  }
  statement {
    sid       = "Catalogar"
    actions   = ["glue:StartCrawler", "glue:GetCrawler"]
    resources = ["arn:${local.particion}:glue:${var.region}:${local.cuenta}:crawler/${var.crawler_name}"]
  }
  statement {
    sid     = "EscribirLogs"
    actions = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
    resources = [
      "arn:${local.particion}:logs:${var.region}:${local.cuenta}:log-group:${var.log_group_jobs}:*",
      "arn:${local.particion}:logs:${var.region}:${local.cuenta}:log-group:${var.log_group_spark}:*",
      "arn:${local.particion}:logs:${var.region}:${local.cuenta}:log-group:${var.log_group_ssm}:*",
    ]
  }
  statement {
    sid       = "MetricasPropias"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # PutMetricData no admite ARN; se restringe por espacio de nombres.
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["LatamOps", "CWAgent"]
    }
  }
  statement {
    sid       = "EtiquetasParaAgente"
    actions   = ["ec2:DescribeTags"]
    resources = ["*"] # Requerido por el agente de CloudWatch; no admite ARN.
  }
  statement {
    sid       = "NotificarDesdeJobs"
    actions   = ["sns:Publish"]
    resources = [var.sns_topic_arn]
  }
}

resource "aws_iam_role_policy" "nodo" {
  name   = "lake-y-operacion"
  role   = aws_iam_role.nodo.id
  policy = data.aws_iam_policy_document.nodo.json
}

resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.nodo.name
  policy_arn = "arn:${local.particion}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "nodo" {
  name = "${var.proyecto}-nodo"
  role = aws_iam_role.nodo.name
}
