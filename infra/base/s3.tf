# Data lake: un solo bucket con zonas bronze / silver / gold y prefijos operativos.

resource "aws_s3_bucket" "lake" {
  #checkov:skip=CKV_AWS_144:Replicacion entre regiones fuera del presupuesto del proyecto (crédito US$100); el dato crudo es re-descargable desde la fuente oficial.
  #checkov:skip=CKV_AWS_145:SSE-S3 (AES256) elegido a proposito; KMS CMK prohibido por costo en las reglas del proyecto.
  #checkov:skip=CKV_AWS_18:Sin bucket de logs de acceso separado (costo); la auditoria se cubre con CloudTrail del historial de eventos.
  #checkov:skip=CKV2_AWS_62:Las notificaciones se envian a EventBridge (recurso aws_s3_bucket_notification).
  bucket        = local.bucket_nombre
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

data "aws_iam_policy_document" "lake_solo_tls" {
  statement {
    sid     = "DenegarSinTLS"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.lake.arn,
      "${aws_s3_bucket.lake.arn}/*",
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket     = aws_s3_bucket.lake.id
  policy     = data.aws_iam_policy_document.lake_solo_tls.json
  depends_on = [aws_s3_bucket_public_access_block.lake]
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "scale-test-7-dias"
    status = "Enabled"
    filter {
      prefix = "scale-test/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "athena-results-7-dias"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "logs-30-dias"
    status = "Enabled"
    filter {
      prefix = "logs/"
    }
    expiration {
      days = 30
    }
  }

  rule {
    id     = "landing-30-dias"
    status = "Enabled"
    filter {
      prefix = "bronze/landing/"
    }
    expiration {
      days = 30
    }
  }

  rule {
    id     = "versiones-y-multipart"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.lake]
}

# Prefijos "carpeta" para que la estructura sea visible en la consola.
resource "aws_s3_object" "prefijos" {
  for_each = toset(["bronze/", "silver/", "gold/", "scripts/", "logs/", "tfstate/", "athena-results/", "scale-test/"])
  bucket   = aws_s3_bucket.lake.id
  key      = each.value
  content  = ""
}

# Envia los eventos del bucket a EventBridge (la regla filtra bronze/landing/).
# Se crea despues de los prefijos para que esos objetos vacios no disparen el pipeline.
resource "aws_s3_bucket_notification" "lake" {
  bucket      = aws_s3_bucket.lake.id
  eventbridge = true
  depends_on  = [aws_s3_object.prefijos]
}
