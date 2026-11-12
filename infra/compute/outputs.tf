output "master_instance_id" {
  description = "ID de la instancia master (destino de los tuneles SSM y de SSM Run Command)."
  value       = aws_instance.master.id
}

output "worker_instance_ids" {
  description = "IDs de los workers."
  value       = aws_instance.worker[*].id
}

output "master_private_ip" {
  description = "IP privada del master (spark://<ip>:7077)."
  value       = aws_instance.master.private_ip
}

output "comandos_tunel" {
  description = "Comandos listos para abrir los tuneles SSM (o usar make tunel / make history)."
  value = {
    jupyter         = "scripts/tunel_ssm.sh 8888 8888"
    spark_master_ui = "scripts/tunel_ssm.sh 8080 8080"
    spark_app_ui    = "scripts/tunel_ssm.sh 4040 4040"
    history_server  = "scripts/tunel_ssm.sh 18080 18080"
  }
}

output "costo_estimado_hora_usd" {
  description = "Estimacion (sin verificar con Pricing Calculator) del costo por hora encendido: EC2 + IPv4 publica por nodo."
  value       = format("~%.2f USD/h con %d nodo(s)", (1 + var.worker_count) * (0.1008 + 0.005), 1 + var.worker_count)
}
