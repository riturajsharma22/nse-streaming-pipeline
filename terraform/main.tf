locals {
  account_id    = "163649804794"
  raw_bucket    = "nse-stream-raw-rituraj-sg-4821"
  function_name = "nse-producer-test"

  # Values issued by Snowflake (DESC INTEGRATION S3_NSE_INT / SHOW PIPES)
  snowflake_iam_user_arn = "arn:aws:iam::783722568070:user/19ce2000-s"
  snowflake_external_id  = "WI27432_SFCRole=2_SjBvvVTx2tKgiR1KpvgV4Nd6Bzo="
  snowpipe_queue_arn     = "arn:aws:sqs:ap-southeast-1:783722568070:sf-snowpipe-AIDA3M6MBLGDBMYFJ6YF4-HJGmu-DMY9f_GrjEnu2T7g"
}

# ---------------- S3 raw landing bucket (Singapore) ----------------

resource "aws_s3_bucket" "raw" {
  provider = aws.singapore
  bucket   = local.raw_bucket
}

resource "aws_s3_bucket_public_access_block" "raw" {
  provider = aws.singapore
  bucket   = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# New file in raw/stocks/ -> event to Snowpipe's SQS queue -> auto-ingest
resource "aws_s3_bucket_notification" "snowpipe" {
  provider = aws.singapore
  bucket   = aws_s3_bucket.raw.id

  queue {
    id            = "snowpipe-autoingest"
    queue_arn     = local.snowpipe_queue_arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/stocks/"
    filter_suffix = ".json"
  }
}

# ---------------- Producer Lambda (Mumbai) ----------------

data "archive_file" "producer" {
  type        = "zip"
  source_file = "${path.module}/../lambda/nse_producer/lambda_function.py"
  output_path = "${path.module}/build/nse_producer.zip"
}

resource "aws_iam_role" "producer_lambda" {
  name = "nse-producer-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "producer_lambda" {
  name = "nse-producer-permissions"
  role = aws_iam_role.producer_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "WriteRawFilesOnly"
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "arn:aws:s3:::${local.raw_bucket}/raw/*"
      },
      {
        Sid      = "CreateLogGroup"
        Effect   = "Allow"
        Action   = "logs:CreateLogGroup"
        Resource = "arn:aws:logs:ap-south-1:${local.account_id}:*"
      },
      {
        Sid      = "WriteOwnLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:ap-south-1:${local.account_id}:log-group:/aws/lambda/${local.function_name}:*"
      }
    ]
  })
}

resource "aws_lambda_function" "producer" {
  function_name    = local.function_name
  role             = aws_iam_role.producer_lambda.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.13"
  timeout          = 30
  memory_size      = 128
  filename         = data.archive_file.producer.output_path
  source_code_hash = data.archive_file.producer.output_base64sha256

  environment {
    variables = {
      BUCKET_NAME = local.raw_bucket
    }
  }

  # Make sure the new role has its permissions before Lambda switches to it
  depends_on = [aws_iam_role_policy.producer_lambda]
}

# ---------------- Scheduler: every minute, market hours (Mumbai) ----------------

resource "aws_iam_role" "scheduler" {
  name = "nse-producer-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "aws:SourceAccount" = local.account_id }
      }
    }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "invoke-nse-producer"
  role = aws_iam_role.scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = [aws_lambda_function.producer.arn, "${aws_lambda_function.producer.arn}:*"]
    }]
  })
}

resource "aws_scheduler_schedule" "producer" {
  name       = "nse-producer-every-minute"
  group_name = "default"

  schedule_expression          = "cron(* 9-15 ? * MON-FRI *)"
  schedule_expression_timezone = "Asia/Calcutta"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.producer.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_event_age_in_seconds = 300
      maximum_retry_attempts       = 2
    }
  }

  depends_on = [aws_iam_role_policy.scheduler]
}

# ---------------- Role Snowflake assumes to read the raw bucket ----------------

resource "aws_iam_role" "snowflake" {
  name = "snowflake-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = local.snowflake_iam_user_arn }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "sts:ExternalId" = local.snowflake_external_id }
      }
    }]
  })
}

resource "aws_iam_policy" "snowflake_read" {
  name = "snowflake-s3-read-raw"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "arn:aws:s3:::${local.raw_bucket}/raw/*"
      },
      {
        Effect    = "Allow"
        Action    = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource  = "arn:aws:s3:::${local.raw_bucket}"
        Condition = { StringLike = { "s3:prefix" = ["raw/*"] } }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "snowflake_read" {
  role       = aws_iam_role.snowflake.name
  policy_arn = aws_iam_policy.snowflake_read.arn
}
