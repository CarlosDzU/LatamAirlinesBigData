# Identidades humanas: grupo del equipo con permisos del proyecto y MFA obligatorio.
# Los usuarios y sus llaves no se crean aqui (se crean en consola y se agregan al grupo).

resource "aws_iam_group" "equipo" {
  name = "${var.proyecto}-equipo"
}

data "aws_iam_policy_document" "proyecto" {
  #checkov:skip=CKV_AWS_111:La escritura (s3:PutObject/DeleteObject) esta limitada al bucket del proyecto
  #checkov:skip=CKV_AWS_356:Solo acciones de lectura/describe que no admiten ARN o se usan sobre recursos efimeros (EC2, ejecuciones SFN, logs)
  statement {
    sid       = "ListarLake"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    sid       = "ObjetosLake"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid = "AthenaWorkgroup"
    actions = [
      "athena:StartQueryExecution", "athena:StopQueryExecution", "athena:GetQueryExecution",
      "athena:GetQueryResults", "athena:GetWorkGroup", "athena:ListQueryExecutions",
    ]
    resources = [aws_athena_workgroup.lake.arn]
  }
  statement {
    sid = "CatalogoGlue"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions", "glue:StartCrawler", "glue:GetCrawler",
    ]
    resources = [
      "arn:${local.particion}:glue:${var.region}:${local.cuenta}:catalog",
      aws_glue_catalog_database.lake.arn,
      "arn:${local.particion}:glue:${var.region}:${local.cuenta}:table/${aws_glue_catalog_database.lake.name}/*",
      aws_glue_crawler.gold.arn,
    ]
  }
  statement {
    sid       = "SesionesSsmEnNodosDelProyecto"
    actions   = ["ssm:StartSession", "ssm:SendCommand"]
    resources = ["arn:${local.particion}:ec2:${var.region}:${local.cuenta}:instance/*"]
    condition {
      test     = "StringEquals"
      variable = "ssm:resourceTag/Project"
      values   = [var.proyecto]
    }
  }
  statement {
    sid     = "DocumentosSsm"
    actions = ["ssm:StartSession", "ssm:SendCommand"]
    resources = [
      "arn:${local.particion}:ssm:${var.region}::document/AWS-StartPortForwardingSession",
      "arn:${local.particion}:ssm:${var.region}::document/AWS-RunShellScript",
    ]
  }
  statement {
    sid = "LecturaOperativa"
    actions = [
      "ec2:DescribeInstances", "ssm:GetCommandInvocation", "ssm:ListCommandInvocations",
      "ssm:TerminateSession", "ssm:ResumeSession", "ce:GetCostAndUsage",
      "cloudwatch:GetDashboard", "cloudwatch:GetMetricData", "logs:FilterLogEvents",
      "logs:GetLogEvents", "states:DescribeExecution", "states:ListExecutions",
    ]
    resources = ["*"]
  }
  statement {
    sid       = "EjecutarPipeline"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.pipeline.arn]
  }
}

resource "aws_iam_policy" "proyecto" {
  name        = "${var.proyecto}-equipo-proyecto"
  description = "Permisos de trabajo diario sobre los recursos del proyecto."
  policy      = data.aws_iam_policy_document.proyecto.json
}

# Politica estandar: sin MFA solo se permite configurar el propio MFA y cambiar la clave.
data "aws_iam_policy_document" "mfa" {
  statement {
    sid = "GestionarPropioMfa"
    actions = [
      "iam:CreateVirtualMFADevice", "iam:EnableMFADevice", "iam:ListMFADevices",
      "iam:ResyncMFADevice", "iam:GetUser", "iam:ChangePassword", "iam:GetAccountPasswordPolicy",
    ]
    resources = [
      "arn:${local.particion}:iam::${local.cuenta}:user/$${aws:username}",
      "arn:${local.particion}:iam::${local.cuenta}:mfa/*",
    ]
  }
  statement {
    sid    = "DenegarTodoSinMfa"
    effect = "Deny"
    not_actions = [
      "iam:CreateVirtualMFADevice", "iam:EnableMFADevice", "iam:GetUser", "iam:ListMFADevices",
      "iam:ListVirtualMFADevices", "iam:ResyncMFADevice", "iam:ChangePassword",
      "iam:GetAccountPasswordPolicy", "sts:GetSessionToken",
    ]
    resources = ["*"]
    condition {
      test     = "BoolIfExists"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["false"]
    }
  }
}

resource "aws_iam_policy" "mfa" {
  name        = "${var.proyecto}-mfa-obligatorio"
  description = "Niega toda accion sin MFA, salvo configurar el propio MFA."
  policy      = data.aws_iam_policy_document.mfa.json
}

resource "aws_iam_group_policy_attachment" "proyecto" {
  group      = aws_iam_group.equipo.name
  policy_arn = aws_iam_policy.proyecto.arn
}

resource "aws_iam_group_policy_attachment" "mfa" {
  group      = aws_iam_group.equipo.name
  policy_arn = aws_iam_policy.mfa.arn
}
