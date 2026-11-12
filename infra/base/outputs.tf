output "bucket_name" {
  description = "Nombre del bucket del data lake (entrada obligatoria de infra/compute)."
  value       = aws_s3_bucket.lake.bucket
}

output "bucket_arn" {
  description = "ARN del bucket del data lake."
  value       = aws_s3_bucket.lake.arn
}

output "glue_database" {
  description = "Base de datos del Glue Data Catalog con las tablas gold."
  value       = aws_glue_catalog_database.lake.name
}

output "crawler_name" {
  description = "Crawler on-demand de la zona gold."
  value       = aws_glue_crawler.gold.name
}

output "athena_workgroup" {
  description = "Workgroup de Athena (usar tambien en el driver ODBC de Power BI)."
  value       = aws_athena_workgroup.lake.name
}

output "sns_topic_arn" {
  description = "Topico SNS de alertas del pipeline."
  value       = aws_sns_topic.alertas.arn
}

output "state_machine_arn" {
  description = "Maquina de estados del pipeline (make pipeline)."
  value       = aws_sfn_state_machine.pipeline.arn
}

output "log_group_jobs" {
  description = "Log group donde los jobs escriben logs JSON."
  value       = aws_cloudwatch_log_group.jobs.name
}

output "log_group_ssm" {
  description = "Log group con la salida de los comandos SSM de cada paso."
  value       = aws_cloudwatch_log_group.ssm.name
}

output "log_group_spark" {
  description = "Log group para los logs de Spark (agente CloudWatch)."
  value       = aws_cloudwatch_log_group.spark.name
}

output "ruta_salt" {
  description = "Parametro SSM de la salt (se crea con make salt)."
  value       = var.ruta_salt
}

output "github_plan_role_arn" {
  description = "Rol que GitHub Actions asume por OIDC para terraform plan."
  value       = var.crear_oidc_github ? aws_iam_role.github_plan[0].arn : null
}

output "grupo_equipo" {
  description = "Grupo IAM al que se agregan los usuarios del equipo (creados en consola, con MFA)."
  value       = aws_iam_group.equipo.name
}
