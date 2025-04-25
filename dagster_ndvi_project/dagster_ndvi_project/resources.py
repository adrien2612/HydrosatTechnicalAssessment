# dagster_ndvi_project/resources.py

from dagster import ConfigurableResource, EnvVar
import boto3

class MinioResource(ConfigurableResource):
    """A resource that provides an S3 client configured for MinIO."""
    endpoint_url: str = EnvVar("MINIO_ENDPOINT_URL")
    aws_access_key_id: str = EnvVar("MINIO_ACCESS_KEY")
    aws_secret_access_key: str = EnvVar("MINIO_SECRET_KEY")
    bucket_name: str = EnvVar("MINIO_BUCKET_NAME")

    def get_s3_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            config=boto3.session.Config(s3={"addressing_style": "path"}),
            verify=False if "localhost" in self.endpoint_url else True,
        )

    def ensure_bucket_structure(self):
        s3 = boto3.resource(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            config=boto3.session.Config(s3={"addressing_style": "path"}),
            verify=False if "localhost" in self.endpoint_url else True,
        )
        bucket = s3.Bucket(self.bucket_name)
        try:
            s3.meta.client.head_bucket(Bucket=self.bucket_name)
        except:
            bucket.create()
        # Make sure prefixes exist
        for prefix in ["input_data/", "input_data/processed_data/", "output_data/"]:
            s3.Object(self.bucket_name, prefix).put(Body=b"")
        return bucket
