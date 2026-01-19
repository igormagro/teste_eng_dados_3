############################# Define provedor AWS (us-east-1)
provider "aws" {
  region = "us-east-1"
  
  default_tags {
    tags = {
      "projeto" = "teste_eng_dados"
    }
  }
}

############################# Assumindo que os buckets "Bronze" e "Silver" já existem
data "aws_s3_bucket" "bronze" {
    bucket = "bucket-bronze"
}
data "aws_s3_bucket" "silver" {
    bucket = "bucket-silver"
}

############################# Define IAM Role para Glue
resource "aws_iam_role" "glue_service_role" {
  name = "teste-eng-dados-glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
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

# Política para acesso aos buckets Bronze e Silver
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

################### Cria bucket e faz upload do script Python para o bucket S3
resource "aws_s3_bucket" "artifacts" {
  bucket = "teste-eng-dados-artifacts"
}

resource "aws_s3_object" "process_job" {
  bucket = aws_s3_bucket.artifacts.bucket

  source = "${path.module}/1.ETL/script.py"
  key    = "glue/jobs/process_client_data.py"

  # checa alterações no arquivo para atualizar o objeto no S3
  source_hash = filemd5("${path.module}/1.ETL/script.py")
}

############################# Define Glue Job
resource "aws_glue_job" "process_job" {
  name     = "process-client-data-job"
  role_arn = aws_iam_role.glue_service_role.arn

  glue_version      = "5.0"
  number_of_workers = 10
  worker_type       = "G.1X"

  command {
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/${aws_s3_object.process_job.key}"
    python_version  = "3"
  }
}