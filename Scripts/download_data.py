import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from config import S3_BUCKET, ACCESS_KEY, SECRET_KEY, DATA_DIR

def download_from_s3():
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
        )
        os.makedirs(DATA_DIR, exist_ok=True)

        # List files in the S3 bucket
        objects = s3.list_objects_v2(Bucket=S3_BUCKET).get("Contents", [])
        for obj in objects:
            file_path = os.path.join(DATA_DIR, obj["Key"])
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            s3.download_file(S3_BUCKET, obj["Key"], file_path)
            print(f"Downloaded: {file_path}")

    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    download_from_s3()
