# Catalogo de la zona gold y motor de consulta SQL.

resource "aws_glue_catalog_database" "lake" {
  name        = local.glue_db
  description = "Tablas de la zona gold del lake ${local.bucket_nombre}"
}

data "aws_iam_policy_document" "glue_confianza" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "crawler" {
  name               = "${var.proyecto}-crawler"
  assume_role_policy = data.aws_iam_policy_document.glue_confianza.json
}

resource "aws_iam_role_policy_attachment" "crawler_servicio" {
  role       = aws_iam_role.crawler.name
  policy_arn = "arn:${local.particion}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "crawler_gold" {
  statement {
    sid       = "LeerGold"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/gold/*"]
  }
  statement {
    sid       = "ListarGold"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["gold", "gold/*"]
    }
  }
}

resource "aws_iam_role_policy" "crawler_gold" {
  name   = "leer-gold"
  role   = aws_iam_role.crawler.id
  policy = data.aws_iam_policy_document.crawler_gold.json
}

# Crawler bajo demanda (sin schedule): lo lanza el paso Catalogar o `make catalogar`.
# TableLevelConfiguration = 2 -> cada carpeta gold/<tabla>/ es una tabla; year=/month= son particiones.
resource "aws_glue_security_configuration" "lake" {
  #checkov:skip=CKV_AWS_99:S3 cifrado con SSE-S3; CloudWatch y bookmarks sin KMS CMK (prohibido por costo y sin jobs ETL)
  name = "${var.proyecto}-seguridad"
  encryption_configuration {
    s3_encryption {
      s3_encryption_mode = "SSE-S3"
    }
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "DISABLED" # KMS CMK prohibido por costo
    }
    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "DISABLED" # sin jobs ETL de Glue
    }
  }
}

resource "aws_glue_crawler" "gold" {
  security_configuration = aws_glue_security_configuration.lake.name
  name                   = "${var.proyecto}-gold"
  database_name          = aws_glue_catalog_database.lake.name
  role                   = aws_iam_role.crawler.arn

  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/gold/"
    exclusions = [
      "modelos/**",
      "metricas/**",
      "_*/**",
      "**/_linaje.json",
      "**/_SUCCESS",
      "*.json",
    ]
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version  = 1.0
    Grouping = { TableLevelConfiguration = 2 }
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}

resource "aws_athena_workgroup" "lake" {
  name          = var.proyecto
  force_destroy = var.force_destroy

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_limite_bytes

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
