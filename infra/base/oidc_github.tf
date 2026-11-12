# CI sin secretos: GitHub Actions asume este rol por OIDC solo para `terraform plan` (lectura).
# El apply es siempre manual y local.

resource "aws_iam_openid_connect_provider" "github" {
  count          = var.crear_oidc_github ? 1 : 0
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # Huellas publicadas para el emisor de GitHub; AWS ademas valida contra su almacen de CA.
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
  ]
}

data "aws_iam_policy_document" "github_confianza" {
  count = var.crear_oidc_github ? 1 : 0
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github[0].arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:*"]
    }
  }
}

resource "aws_iam_role" "github_plan" {
  count                = var.crear_oidc_github ? 1 : 0
  name                 = "${var.proyecto}-github-plan"
  assume_role_policy   = data.aws_iam_policy_document.github_confianza[0].json
  max_session_duration = 3600
}

resource "aws_iam_role_policy_attachment" "github_lectura" {
  count      = var.crear_oidc_github ? 1 : 0
  role       = aws_iam_role.github_plan[0].name
  policy_arn = "arn:${local.particion}:iam::aws:policy/ReadOnlyAccess"
}

# El plan de compute lee su estado en S3 y escribe/borra el archivo de bloqueo (use_lockfile).
data "aws_iam_policy_document" "github_estado" {
  statement {
    sid       = "LeerEstado"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/tfstate/*"]
  }
  statement {
    sid       = "BloqueoEstado"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/tfstate/*.tflock"]
  }
}

resource "aws_iam_role_policy" "github_estado" {
  count  = var.crear_oidc_github ? 1 : 0
  name   = "estado-terraform"
  role   = aws_iam_role.github_plan[0].id
  policy = data.aws_iam_policy_document.github_estado.json
}
