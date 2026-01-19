########################################
# Provider
########################################
provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      projeto = "teste_eng_dados"
    }
  }
}

########################################
# S3 Buckets (existentes)
# - Bronze
# - Silver
########################################
data "aws_s3_bucket" "bronze" {
  bucket = "bucket-bronze"
}

data "aws_s3_bucket" "silver" {
  bucket = "bucket-silver"
}

########################################
# IAM Role - AWS Glue
########################################
resource "aws_iam_role" "glue_service_role" {
  name = "glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Permissões de acesso aos buckets Bronze e Silver
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue-s3-bronze-silver-access"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${data.aws_s3_bucket.bronze.arn}/*",
          "${data.aws_s3_bucket.silver.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          data.aws_s3_bucket.bronze.arn,
          data.aws_s3_bucket.silver.arn
        ]
      }
    ]
  })
}

########################################
# Bucket de Artefatos (scripts Glue)
########################################
resource "aws_s3_bucket" "artifacts" {
  bucket = "eng-dados-artifacts"
}

# Upload do script Python do Glue Job
resource "aws_s3_object" "job" {
  bucket = aws_s3_bucket.artifacts.bucket

  source = "${path.module}/AnaliseDados/script.py"
  key    = "glue/jobs/data_analysis.py"

  # Atualiza o objeto no S3 sempre que o script local mudar
  source_hash = filemd5("${path.module}/AnaliseDados/script.py")
}

########################################
# AWS Glue Job
########################################
resource "aws_glue_job" "job" {
  name     = "data-analysis-job"
  role_arn = aws_iam_role.glue_service_role.arn

  glue_version      = "5.0"
  number_of_workers = 10
  worker_type       = "G.1X"

  command {
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/${aws_s3_object.job.key}"
    python_version  = "3"
  }
}
