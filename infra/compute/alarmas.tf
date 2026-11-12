# Auto-apagado: si la CPU promedio queda < 5 % durante idle_stop_minutes, CloudWatch detiene
# la instancia (accion ec2:stop). Es la red de seguridad si se olvida `make compute-down`.

locals {
  nodos = merge(
    { master = aws_instance.master.id },
    { for i, w in aws_instance.worker : "worker-${i + 1}" => w.id },
  )
}

resource "aws_cloudwatch_metric_alarm" "ocioso" {
  for_each            = local.nodos
  alarm_name          = "${var.proyecto}-${each.key}-ocioso"
  alarm_description   = "Detiene ${each.key} tras ${var.idle_stop_minutes} min con CPU < 5 %."
  namespace           = "AWS/EC2"
  metric_name         = "CPUUtilization"
  dimensions          = { InstanceId = each.value }
  statistic           = "Average"
  period              = 900
  evaluation_periods  = var.idle_stop_minutes / 15
  threshold           = 5
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "missing"
  alarm_actions       = ["arn:${local.particion}:automate:${var.region}:ec2:stop", var.sns_topic_arn]
}
