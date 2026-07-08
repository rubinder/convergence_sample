resource "aws_emrserverless_application" "spark" {
  name          = "${var.project}-spark"
  release_label = "emr-7.1.0"
  type          = "spark"

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 5
  }
}
