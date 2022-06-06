terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.27"
    }
  }

  required_version = ">= 0.14.9"
}

provider "aws" {
  profile = "default"
  region  = "us-east-1"
}

resource "aws_key_pair" "aws-kp1" {
  # key_name   = "aws-kp1"
  public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCOusfjlbVsW2NNxpzpTmz+rr5IRGpFABMpdnzaQe1SQwLkwN3N8MklGgsYti5dpkYXJFj4AmHOkjY+bO/ktA3w6+EQK6J9dmGpJo+1+u+qHprCdUIMU9UIzcpq0mHgqEBXAVmw3oSh5uFtjNbzxeEsaKbvxi1i5Qr3aAVv0T4nvr0PppDQnhgu/Z9Pe6KxV7dGLCWQf0U6eMpHbGpslELByvM/T1fCLZPL8vBKprbMrSK//DghPIkOhStJVKxDnWuTtpwIch209vtovBCQlOlI5BrDKcL23/cwSq6pcy3NaL32WGPtysNmLvfyQDCr+G+OCNzcwQRLv85SSmPSyGWX"
}

resource "aws_instance" "app_server" {
  ami           = "ami-09d56f8956ab235b3"
  instance_type = "t2.micro"
  key_name = "aws-kp1"

  tags = {
    Name = "ExampleAppServerInstance"
  }
}

output "app_public_dns" {
  value = aws_instance.app_server.public_dns
}