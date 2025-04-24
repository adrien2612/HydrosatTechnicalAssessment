# Placeholder for Dagster resources 

from dagster import ConfigurableResource, EnvVar
from dagster_aws.s3 import S3Resource
import boto3
import os


class MinioResource(ConfigurableResource):
    """A resource that provides an S3 client configured to connect to MinIO."""
    endpoint_url: str = EnvVar("MINIO_ENDPOINT_URL")
    aws_access_key_id: str = EnvVar("MINIO_ACCESS_KEY") 
    aws_secret_access_key: str = EnvVar("MINIO_SECRET_KEY")
    bucket_name: str = EnvVar("MINIO_BUCKET_NAME")
    
    def get_s3_client(self):
        """Get a boto3 S3 client configured for MinIO."""
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            # For MinIO, we need to disable virtual-hosted-style S3 URLs
            config=boto3.session.Config(s3={"addressing_style": "path"}),
            # Verify=False if using self-signed certificates with local MinIO
            verify=False if "localhost" in self.endpoint_url or "127.0.0.1" in self.endpoint_url else True,
        )
    
    def get_s3_resource(self):
        """Get a boto3 S3 resource configured for MinIO."""
        return boto3.resource(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            # For MinIO, we need to disable virtual-hosted-style S3 URLs
            config=boto3.session.Config(s3={"addressing_style": "path"}),
            # Verify=False if using self-signed certificates with local MinIO
            verify=False if "localhost" in self.endpoint_url or "127.0.0.1" in self.endpoint_url else True,
        )
    
    def ensure_bucket_structure(self):
        """Ensure that the required bucket and folder structure exists."""
        s3 = self.get_s3_resource()
        bucket = s3.Bucket(self.bucket_name)
        
        # Check if bucket exists, create if it doesn't
        try:
            s3.meta.client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket {self.bucket_name} exists")
        except:
            print(f"Creating bucket {self.bucket_name}")
            bucket.create()
        
        # Ensure input_data folder exists
        s3.Object(self.bucket_name, 'input_data/').put(Body='')
        # Ensure input_data/processed_data folder exists
        s3.Object(self.bucket_name, 'input_data/processed_data/').put(Body='')
        # Ensure output_data folder exists
        s3.Object(self.bucket_name, 'output_data/').put(Body='')
        
        print(f"Bucket structure verified for {self.bucket_name}")
        return bucket 