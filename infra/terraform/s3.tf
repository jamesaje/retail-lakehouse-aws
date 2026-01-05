locals {
  project_prefix = "femi-lakehouse-usw1"
}

resource "aws_s3_bucket" "raw" {
  bucket = "${local.project_prefix}-raw"
}

resource "aws_s3_bucket" "cleaned" {
  bucket = "${local.project_prefix}-cleaned"
}

resource "aws_s3_bucket" "curated" {
  bucket = "${local.project_prefix}-curated"
}

# Optional: enforce bucket-level encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "sse" {
  for_each = {
    raw     = aws_s3_bucket.raw.id
    cleaned = aws_s3_bucket.cleaned.id
    curated = aws_s3_bucket.curated.id
  }

  bucket = each.value

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
