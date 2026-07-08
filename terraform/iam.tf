# ---- EMR Serverless job role ----
data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_job" {
  name               = "${var.project}-emr-job-role"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

resource "aws_iam_role_policy" "emr_job" {
  role = aws_iam_role.emr_job.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:*", "glue:*", "athena:*", "lakeformation:GetDataAccess", "logs:*"],
      Resource = "*"
    }]
  })
}

# ---- Lambda role (reach tools) ----
resource "aws_iam_role" "lambda" {
  name = "${var.project}-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["athena:*", "glue:*", "s3:*", "logs:*"],
      Resource = "*"
    }]
  })
}
