resource "aws_lambda_function" "reach_tools" {
  function_name    = "${var.project}-reach-tools"
  role             = aws_iam_role.lambda.arn
  handler          = "agent.reach_tools_lambda.handler"
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 256
  filename         = "${path.module}/../build/reach_tools.zip"
  source_code_hash = filebase64sha256("${path.module}/../build/reach_tools.zip")

  environment {
    variables = {
      ATHENA_WG = "${var.project}-wg"
    }
  }
}
