# Orquestacion serverless: Step Functions ejecuta los pasos en el master via SSM Run Command.
# Disparo automatico: EventBridge cuando llega un archivo a bronze/landing/ (demo de punta a punta).
# Disparo manual: `make pipeline` (states:StartExecution con {"objeto": "..."}).

data "aws_iam_policy_document" "sfn_confianza" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.cuenta]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${var.proyecto}-sfn"
  assume_role_policy = data.aws_iam_policy_document.sfn_confianza.json
}

data "aws_iam_policy_document" "sfn" {
  statement {
    sid       = "BuscarMaster"
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"] # DescribeInstances no admite restriccion por recurso.
  }
  statement {
    sid       = "EnviarComandoANodosDelProyecto"
    actions   = ["ssm:SendCommand"]
    resources = ["arn:${local.particion}:ec2:${var.region}:${local.cuenta}:instance/*"]
    condition {
      test     = "StringEquals"
      variable = "ssm:resourceTag/Project"
      values   = [var.proyecto]
    }
  }
  statement {
    sid       = "DocumentoRunShell"
    actions   = ["ssm:SendCommand"]
    resources = ["arn:${local.particion}:ssm:${var.region}::document/AWS-RunShellScript"]
  }
  statement {
    sid       = "EstadoComando"
    actions   = ["ssm:GetCommandInvocation"]
    resources = ["*"] # GetCommandInvocation no admite restriccion por recurso.
  }
  statement {
    sid       = "Catalogar"
    actions   = ["glue:StartCrawler"]
    resources = [aws_glue_crawler.gold.arn]
  }
  statement {
    sid       = "Notificar"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alertas.arn]
  }
  statement {
    sid = "EntregaDeLogs"
    actions = [
      "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies", "logs:DescribeLogGroups",
    ]
    resources = ["*"] # Las APIs de entrega de logs de Step Functions exigen "*".
  }
}

resource "aws_iam_role_policy" "sfn" {
  name   = "pipeline"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn.json
}

resource "aws_sfn_state_machine" "pipeline" {
  #checkov:skip=CKV_AWS_284:X-Ray omitido por costo; la trazabilidad la dan los logs de SFN y de SSM
  name     = "${var.proyecto}-pipeline"
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"

  definition = templatefile("${path.module}/stepfunctions/pipeline.asl.json", {
    proyecto      = var.proyecto
    sns_topic_arn = aws_sns_topic.alertas.arn
    log_group_ssm = aws_cloudwatch_log_group.ssm.name
    crawler_name  = aws_glue_crawler.gold.name
    pasos         = jsonencode(var.pasos_pipeline)
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tracing_configuration {
    enabled = false # X-Ray no aporta en un flujo de 8 estados y suma costo.
  }

  depends_on = [aws_iam_role_policy.sfn]
}

# --- EventBridge: archivo nuevo en bronze/landing/ -> ejecucion del pipeline -------------------

data "aws_iam_policy_document" "eventos_confianza" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eventos" {
  name               = "${var.proyecto}-eventos"
  assume_role_policy = data.aws_iam_policy_document.eventos_confianza.json
}

data "aws_iam_policy_document" "eventos" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.pipeline.arn]
  }
}

resource "aws_iam_role_policy" "eventos" {
  name   = "iniciar-pipeline"
  role   = aws_iam_role.eventos.id
  policy = data.aws_iam_policy_document.eventos.json
}

resource "aws_cloudwatch_event_rule" "landing" {
  name        = "${var.proyecto}-landing"
  description = "Archivo nuevo en bronze/landing/ dispara el pipeline completo."
  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.lake.bucket] }
      # Exige un punto en el nombre para ignorar los marcadores de carpeta ("bronze/landing/").
      object = { key = [{ wildcard = "bronze/landing/*.*" }] }
    }
  })
  depends_on = [aws_s3_bucket_notification.lake]
}

resource "aws_cloudwatch_event_target" "landing" {
  rule     = aws_cloudwatch_event_rule.landing.name
  arn      = aws_sfn_state_machine.pipeline.arn
  role_arn = aws_iam_role.eventos.arn
}
