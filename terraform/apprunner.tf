# App Runner: public HTTPS dashboard. Requires the image pushed to ECR first.

resource "aws_iam_role" "apprunner_instance" {
  name = "${var.project}-apprunner-instance"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "tasks.apprunner.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "apprunner_instance" {
  role = aws_iam_role.apprunner_instance.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["athena:*", "glue:*", "s3:*", "bedrock:*", "bedrock-agentcore:*"],
      Resource = "*"
    }]
  })
}

resource "aws_iam_role" "apprunner_access" {
  name = "${var.project}-apprunner-access"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "build.apprunner.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "apprunner_ecr" {
  role       = aws_iam_role.apprunner_access.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess"
}

resource "aws_apprunner_service" "dashboard" {
  count        = var.deploy_dashboard ? 1 : 0
  service_name = "${var.project}-dashboard"

  source_configuration {
    authentication_configuration {
      access_role_arn = aws_iam_role.apprunner_access.arn
    }
    image_repository {
      image_identifier      = "${aws_ecr_repository.dashboard.repository_url}:latest"
      image_repository_type = "ECR"
      image_configuration {
        port = "8080"
        runtime_environment_variables = {
          AWS_REGION = var.region
          ATHENA_WG  = "${var.project}-wg"
          AGENT_ARN  = var.agent_arn
        }
      }
    }
    auto_deployments_enabled = true
  }

  instance_configuration {
    cpu               = "256"
    memory            = "512"
    instance_role_arn = aws_iam_role.apprunner_instance.arn
  }

  health_check_configuration {
    path     = "/healthz"
    protocol = "HTTP"
  }
}
