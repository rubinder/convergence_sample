variable "region" {
  default = "us-east-1"
}

variable "project" {
  default = "convergence"
}

variable "viewer_access_hours" {
  description = "How long the read-only viewer credentials stay valid."
  default     = 120 # 5 days
}

variable "deploy_dashboard" {
  description = "Set true only after the dashboard image is pushed to ECR."
  type        = bool
  default     = false
}

variable "agent_arn" {
  description = "Bedrock AgentCore runtime ARN wired into the dashboard chat."
  default     = ""
}

variable "deploy_mwaa" {
  description = "Set true to provision the MWAA environment (slow, ~25 min)."
  type        = bool
  default     = false
}

locals {
  account_id = data.aws_caller_identity.current.account_id
  bucket     = "convergence-lakehouse-${local.account_id}"
}
