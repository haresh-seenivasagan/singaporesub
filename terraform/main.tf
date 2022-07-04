

terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "4.21.0"
    }
  }
}

provider "aws" {
  # Configuration options
  region= "ap-southeast-1"


}

resource "aws_s3_bucket" "bucket" {
bucket = "inital-etl-test-bucket"

tags = {
  Name= "inital_etl"
  Environment = "Dev"
}
  
}
