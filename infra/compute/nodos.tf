# Cluster Spark standalone: 1 master (driver + worker) y 0-2 workers.

resource "aws_instance" "master" {
  #checkov:skip=CKV_AWS_126:Monitoreo detallado (1 min) tiene costo; el basico de 5 min basta para la alarma de auto-apagado.
  #checkov:skip=CKV_AWS_135:m7i-flex ya es EBS-optimized por defecto.
  #checkov:skip=CKV_AWS_88:IP publica necesaria para salir sin NAT; no hay ingreso desde internet (SG) ni llave SSH.
  ami                         = data.aws_ssm_parameter.ami_al2023.value
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.publica.id
  vpc_security_group_ids      = [aws_security_group.cluster.id]
  iam_instance_profile        = aws_iam_instance_profile.nodo.name
  associate_public_ip_address = true
  user_data_replace_on_change = true

  user_data = templatefile("${path.module}/templates/user_data_master.sh.tftpl", {
    bootstrap_comun = local.bootstrap_comun
    spark_defaults  = local.spark_defaults
    bucket          = var.bucket_name
    region          = var.region
    memoria_worker  = var.memoria.worker_en_master
    executor_cores  = var.memoria.executor_cores
    log_group_jobs  = var.log_group_jobs
    sns_topic_arn   = var.sns_topic_arn
    crawler_name    = var.crawler_name
    ruta_salt       = var.ruta_salt
    proyecto        = var.proyecto
  })

  metadata_options {
    http_tokens                 = "required" # IMDSv2
    http_endpoint               = "enabled"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.ebs_gb
    encrypted             = true
    delete_on_termination = true
  }

  tags = { Name = "${var.proyecto}-master", Rol = "master" }

  depends_on = [aws_route_table_association.publica, aws_vpc_endpoint.s3]
}

resource "aws_instance" "worker" {
  #checkov:skip=CKV_AWS_126:Igual que master.
  #checkov:skip=CKV_AWS_135:Igual que master.
  #checkov:skip=CKV_AWS_88:Igual que master.
  count                       = var.worker_count
  ami                         = data.aws_ssm_parameter.ami_al2023.value
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.publica.id
  vpc_security_group_ids      = [aws_security_group.cluster.id]
  iam_instance_profile        = aws_iam_instance_profile.nodo.name
  associate_public_ip_address = true
  user_data_replace_on_change = true

  user_data = templatefile("${path.module}/templates/user_data_worker.sh.tftpl", {
    bootstrap_comun   = local.bootstrap_comun
    spark_defaults    = local.spark_defaults
    master_ip_privada = aws_instance.master.private_ip
    memoria_worker    = var.memoria.worker
    executor_cores    = var.memoria.executor_cores
  })

  metadata_options {
    http_tokens                 = "required"
    http_endpoint               = "enabled"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.ebs_gb
    encrypted             = true
    delete_on_termination = true
  }

  tags = { Name = "${var.proyecto}-worker-${count.index + 1}", Rol = "worker" }
}
