import os
from dotenv import load_dotenv
load_dotenv()
from minio import Minio

MINIO_ENDPOINT = f"localhost:{os.getenv('MINIO_PORT')}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

class MinioClient:
  def __init__(self): 
    self.client = Minio(
      endpoint=MINIO_ENDPOINT,
      access_key=MINIO_ACCESS_KEY,
      secret_key=MINIO_SECRET_KEY,
      secure=False
    )

  def create_bucket_if_not_exists(self, bucket_name: str):
    if not self.client.bucket_exists(bucket_name):
        self.client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket {bucket_name} already exists")

  def delete_bucket_if_not_exists(self, bucket_name: str):
    if self.client.bucket_exists(bucket_name):
        self.client.remove_bucket(bucket_name)
        print(f"Deleted bucket: {bucket_name}")
    else:
        print(f"Bucket {bucket_name} not already exists")