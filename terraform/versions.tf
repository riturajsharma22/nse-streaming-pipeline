terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }

  # State lives in S3; use_lockfile prevents two runs changing it at once
  backend "s3" {
    bucket       = "nse-tfstate-rituraj-4821"
    key          = "nse-streaming-pipeline/terraform.tfstate"
    region       = "ap-south-1"
    encrypt      = true
    use_lockfile = true
  }
}

# Mumbai: Lambda, Scheduler, state bucket (IAM is global)
provider "aws" {
  region = "ap-south-1"

  default_tags {
    tags = {
      project    = "nse-streaming-pipeline"
      managed_by = "terraform"
    }
  }
}

# Singapore: raw bucket, same region as Snowflake for Snowpipe auto-ingest
provider "aws" {
  alias  = "singapore"
  region = "ap-southeast-1"

  default_tags {
    tags = {
      project    = "nse-streaming-pipeline"
      managed_by = "terraform"
    }
  }
}
