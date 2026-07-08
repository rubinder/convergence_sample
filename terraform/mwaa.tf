# MWAA (managed Airflow) with a dedicated VPC (2 private + 2 public subnets, 1 NAT).
# Gated behind var.deploy_mwaa because provisioning takes ~25 min and adds cost.
# MWAA requires PRIVATE worker subnets with NAT egress — the default VPC's public
# subnets do not qualify, so we create a minimal purpose-built VPC.

data "aws_availability_zones" "available" {
  count = var.deploy_mwaa ? 1 : 0
  state = "available"
}

resource "aws_vpc" "mwaa" {
  count                = var.deploy_mwaa ? 1 : 0
  cidr_block           = "10.20.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${var.project}-mwaa-vpc" }
}

resource "aws_internet_gateway" "mwaa" {
  count  = var.deploy_mwaa ? 1 : 0
  vpc_id = aws_vpc.mwaa[0].id
}

resource "aws_subnet" "public" {
  count             = var.deploy_mwaa ? 2 : 0
  vpc_id            = aws_vpc.mwaa[0].id
  cidr_block        = "10.20.${count.index}.0/24"
  availability_zone = data.aws_availability_zones.available[0].names[count.index]
  tags              = { Name = "${var.project}-public-${count.index}" }
}

resource "aws_subnet" "private" {
  count             = var.deploy_mwaa ? 2 : 0
  vpc_id            = aws_vpc.mwaa[0].id
  cidr_block        = "10.20.${count.index + 10}.0/24"
  availability_zone = data.aws_availability_zones.available[0].names[count.index]
  tags              = { Name = "${var.project}-private-${count.index}" }
}

resource "aws_eip" "nat" {
  count  = var.deploy_mwaa ? 1 : 0
  domain = "vpc"
}

resource "aws_nat_gateway" "mwaa" {
  count         = var.deploy_mwaa ? 1 : 0
  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public[0].id
  depends_on    = [aws_internet_gateway.mwaa]
}

resource "aws_route_table" "public" {
  count  = var.deploy_mwaa ? 1 : 0
  vpc_id = aws_vpc.mwaa[0].id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.mwaa[0].id
  }
}

resource "aws_route_table" "private" {
  count  = var.deploy_mwaa ? 1 : 0
  vpc_id = aws_vpc.mwaa[0].id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.mwaa[0].id
  }
}

resource "aws_route_table_association" "public" {
  count          = var.deploy_mwaa ? 2 : 0
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public[0].id
}

resource "aws_route_table_association" "private" {
  count          = var.deploy_mwaa ? 2 : 0
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[0].id
}

resource "aws_security_group" "mwaa" {
  count  = var.deploy_mwaa ? 1 : 0
  name   = "${var.project}-mwaa-sg"
  vpc_id = aws_vpc.mwaa[0].id
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# DAGs + requirements live under the lake bucket
resource "aws_s3_object" "dags_keep" {
  count   = var.deploy_mwaa ? 1 : 0
  bucket  = aws_s3_bucket.lake.id
  key     = "dags/.keep"
  content = "keep"
}

resource "aws_iam_role" "mwaa" {
  count = var.deploy_mwaa ? 1 : 0
  name  = "${var.project}-mwaa-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"] },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "mwaa" {
  count = var.deploy_mwaa ? 1 : 0
  role  = aws_iam_role.mwaa[0].id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = ["s3:*", "emr-serverless:*", "athena:*", "glue:*", "logs:*",
      "iam:PassRole", "airflow:*", "cloudwatch:*"],
      Resource = "*"
    }]
  })
}

resource "aws_mwaa_environment" "env" {
  count                = var.deploy_mwaa ? 1 : 0
  name                 = "${var.project}-mwaa"
  airflow_version      = "2.9.2"
  environment_class    = "mw1.small"
  execution_role_arn   = aws_iam_role.mwaa[0].arn
  source_bucket_arn    = aws_s3_bucket.lake.arn
  dag_s3_path          = "dags/"
  requirements_s3_path = "dags/requirements.txt"
  max_workers          = 2

  network_configuration {
    security_group_ids = [aws_security_group.mwaa[0].id]
    subnet_ids         = [aws_subnet.private[0].id, aws_subnet.private[1].id]
  }

  webserver_access_mode = "PUBLIC_ONLY"

  logging_configuration {
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  depends_on = [aws_s3_object.dags_keep]
}
