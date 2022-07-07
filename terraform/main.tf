

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
  
resource "aws_s3_bucket" "load_bucket" {
bucket = "initial-data-load-bucket"

tags = {
  Name= "inital_load"
  Environment = "Dev"
}
}
resource "aws_s3_bucket" "comm_bucket" {
bucket = "initial-comments-load-bucket"

tags = {
  Name= "inital_load"
  Environment = "Dev"
}
}
resource "aws_s3_bucket" "comm_trans_bucket" {
bucket = "comments-transformed-bucket"

tags = {
  Name= "with_transformation"
  Environment = "Dev"
}
}
