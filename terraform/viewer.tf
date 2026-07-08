# Read-only reviewer access to the AWS console, auto-expiring after viewer_access_hours.
# Lets a hiring reviewer inspect the real infrastructure (S3, Glue, Athena, EMR, MWAA,
# Lambda, App Runner) without any write ability, and access lapses automatically.

resource "aws_iam_user" "viewer" {
  name          = "${var.project}-viewer"
  force_destroy = true
}

resource "aws_iam_user_policy_attachment" "viewer_readonly" {
  user       = aws_iam_user.viewer.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

# Time-boxed kill switch: deny everything once the window elapses.
resource "aws_iam_user_policy" "viewer_expiry" {
  name = "expire-access"
  user = aws_iam_user.viewer.name
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Sid      = "DenyAfterExpiry",
      Effect   = "Deny",
      Action   = "*",
      Resource = "*",
      Condition = {
        DateGreaterThan = {
          "aws:CurrentTime" = timeadd(timestamp(), "${var.viewer_access_hours}h")
        }
      }
    }]
  })

  lifecycle {
    ignore_changes = [policy] # don't let the expiry timestamp drift on every apply
  }
}

resource "aws_iam_user_login_profile" "viewer" {
  user                    = aws_iam_user.viewer.name
  password_length         = 20
  password_reset_required = false
}

output "viewer_username" {
  value = aws_iam_user.viewer.name
}

output "viewer_console_url" {
  value = "https://${local.account_id}.signin.aws.amazon.com/console"
}

output "viewer_password" {
  value     = aws_iam_user_login_profile.viewer.password
  sensitive = true
}

output "viewer_access_expires_utc" {
  value = timeadd(timestamp(), "${var.viewer_access_hours}h")
}
