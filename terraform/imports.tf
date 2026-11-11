# Adopt resources originally created by hand in the AWS console.
# Terraform reads them into its state instead of creating duplicates.

import {
  provider = aws.singapore
  to       = aws_s3_bucket.raw
  id       = "nse-stream-raw-rituraj-sg-4821"
}

import {
  provider = aws.singapore
  to       = aws_s3_bucket_public_access_block.raw
  id       = "nse-stream-raw-rituraj-sg-4821"
}

import {
  provider = aws.singapore
  to       = aws_s3_bucket_notification.snowpipe
  id       = "nse-stream-raw-rituraj-sg-4821"
}

import {
  to = aws_lambda_function.producer
  id = "nse-producer-test"
}

import {
  to = aws_scheduler_schedule.producer
  id = "default/nse-producer-every-minute"
}

import {
  to = aws_iam_role.snowflake
  id = "snowflake-s3-role"
}

import {
  to = aws_iam_policy.snowflake_read
  id = "arn:aws:iam::163649804794:policy/snowflake-s3-read-raw"
}

import {
  to = aws_iam_role_policy_attachment.snowflake_read
  id = "snowflake-s3-role/arn:aws:iam::163649804794:policy/snowflake-s3-read-raw"
}
