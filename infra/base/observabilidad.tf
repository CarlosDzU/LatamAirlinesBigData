# Observabilidad: log groups, dashboard y alarmas que avisan por SNS.

resource "aws_cloudwatch_log_group" "jobs" {
  #checkov:skip=CKV_AWS_158:Cifrado de logs con KMS CMK prohibido por costo; CloudWatch Logs ya cifra en reposo con claves de AWS.
  #checkov:skip=CKV_AWS_338:Retencion corta (14 dias) a proposito para el presupuesto; las evidencias se copian a S3 y al repo.
  name              = "/${var.proyecto}/jobs"
  retention_in_days = var.retencion_logs_dias
}

resource "aws_cloudwatch_log_group" "ssm" {
  #checkov:skip=CKV_AWS_158:Igual que jobs.
  #checkov:skip=CKV_AWS_338:Igual que jobs.
  name              = "/${var.proyecto}/ssm"
  retention_in_days = var.retencion_logs_dias
}

resource "aws_cloudwatch_log_group" "sfn" {
  #checkov:skip=CKV_AWS_158:Igual que jobs.
  #checkov:skip=CKV_AWS_338:Igual que jobs.
  name              = "/aws/vendedlogs/states/${var.proyecto}-pipeline"
  retention_in_days = var.retencion_logs_dias
}

resource "aws_cloudwatch_log_group" "spark" {
  #checkov:skip=CKV_AWS_158:Igual que jobs.
  #checkov:skip=CKV_AWS_338:Igual que jobs.
  name              = "/${var.proyecto}/spark"
  retention_in_days = var.retencion_logs_dias
}

resource "aws_cloudwatch_metric_alarm" "pipeline_fallido" {
  alarm_name          = "${var.proyecto}-pipeline-fallido"
  alarm_description   = "Alguna ejecucion de la maquina de estados fallo."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.pipeline.arn }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alertas.arn]
}

# Metricas propias publicadas por los jobs con Embedded Metric Format (espacio de nombres LatamOps).
resource "aws_cloudwatch_metric_alarm" "calidad_baja" {
  alarm_name          = "${var.proyecto}-calidad-baja"
  alarm_description   = "Porcentaje de filas que pasan las reglas de calidad por debajo del 95 %."
  namespace           = "LatamOps"
  metric_name         = "pct_calidad"
  statistic           = "Minimum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 95
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alertas.arn]
}

resource "aws_cloudwatch_dashboard" "operacion" {
  dashboard_name = "${var.proyecto}-operacion"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 12, height = 6
        properties = {
          title  = "Ejecuciones del pipeline"
          region = var.region
          stat   = "Sum"
          period = 3600
          metrics = [
            ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", aws_sfn_state_machine.pipeline.arn],
            [".", "ExecutionsFailed", ".", "."],
            [".", "ExecutionTime", ".", ".", { stat = "Average", yAxis = "right" }],
          ]
        }
      },
      {
        type = "metric", x = 12, y = 0, width = 12, height = 6
        properties = {
          title   = "Filas procesadas por paso (EMF)"
          region  = var.region
          stat    = "Sum"
          period  = 3600
          metrics = [[{ expression = "SEARCH('{LatamOps,paso} MetricName=\"filas_out\"', 'Sum', 3600)", id = "e1" }]]
        }
      },
      {
        type = "metric", x = 0, y = 6, width = 12, height = 6
        properties = {
          title   = "Duracion por paso (s)"
          region  = var.region
          stat    = "Average"
          period  = 3600
          metrics = [[{ expression = "SEARCH('{LatamOps,paso} MetricName=\"segundos\"', 'Average', 3600)", id = "e2" }]]
        }
      },
      {
        type = "metric", x = 12, y = 6, width = 12, height = 6
        properties = {
          title  = "Calidad y modelo"
          region = var.region
          stat   = "Average"
          period = 3600
          metrics = [
            ["LatamOps", "pct_calidad"],
            [".", "auc", { yAxis = "right" }],
          ]
        }
      },
      {
        type = "log", x = 0, y = 12, width = 24, height = 6
        properties = {
          title  = "Ultimos errores de los jobs"
          region = var.region
          query  = "SOURCE '${aws_cloudwatch_log_group.jobs.name}' | fields @timestamp, run_id, paso, mensaje | filter nivel = 'ERROR' | sort @timestamp desc | limit 20"
          view   = "table"
        }
      },
    ]
  })
}
