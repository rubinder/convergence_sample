# ---- Storage, catalog, query, image registry ----

resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_glue_catalog_database" "convergence" {
  name = var.project
}

resource "aws_athena_workgroup" "wg" {
  name          = "${var.project}-wg"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://${local.bucket}/athena-results/"
    }
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }
}

resource "aws_ecr_repository" "dashboard" {
  name         = "${var.project}-dashboard"
  force_delete = true
}
