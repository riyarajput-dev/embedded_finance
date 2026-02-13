import os



aws_credentials = {
    "aws_key": os.getenv("AWS_KEY"),
    "aws_secret": os.getenv("AWS_SECRET_KEY"),
    "aws_region":os.getenv("AWS_REGION")
}

