# Alertas de costo y del pipeline por correo.

resource "aws_sns_topic" "alertas" {
  #checkov:skip=CKV_AWS_26:Cifrado de SNS con KMS CMK prohibido por costo; los mensajes no contienen datos sensibles.
  name = "${var.proyecto}-alertas"
}

# Cada correo debe confirmar la suscripcion desde su bandeja (AWS envia el enlace).
resource "aws_sns_topic_subscription" "correo" {
  for_each  = toset(var.alert_emails)
  topic_arn = aws_sns_topic.alertas.arn
  protocol  = "email"
  endpoint  = each.value
}

locals {
  umbrales_reales = [25, 50, 80]
}

resource "aws_budgets_budget" "mensual" {
  name         = "${var.proyecto}-mensual"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = local.umbrales_reales
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = var.alert_emails
    }
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = var.alert_emails
  }
}
