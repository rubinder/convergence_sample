output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "glue_db" {
  value = aws_glue_catalog_database.convergence.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.wg.name
}

output "ecr_repo_url" {
  value = aws_ecr_repository.dashboard.repository_url
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "emr_job_role_arn" {
  value = aws_iam_role.emr_job.arn
}

output "reach_tools_lambda_arn" {
  value = aws_lambda_function.reach_tools.arn
}

output "service_url" {
  value = var.deploy_dashboard ? "https://${aws_apprunner_service.dashboard[0].service_url}" : "(dashboard not deployed yet)"
}

output "mwaa_url" {
  value = var.deploy_mwaa ? "https://${aws_mwaa_environment.env[0].webserver_url}" : "(mwaa not deployed yet)"
}
