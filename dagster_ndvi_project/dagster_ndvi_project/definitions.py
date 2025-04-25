from dagster import Definitions, EnvVar, FilesystemIOManager, define_asset_job, AssetKey
from dagster_aws.s3 import S3Resource
from dagster_k8s import k8s_job_executor
from dagster._utils import file_relative_path
import os

from dagster_ndvi_project.resources import MinioResource
from dagster_ndvi_project.assets import ndvi_by_field
from dagster_ndvi_project.sensors import ndvi_input_sensor

# Define the Dagster IO managers
local_io_manager = FilesystemIOManager(
    base_dir=file_relative_path(__file__, "../data")
)

# Configure K8s job executor for parallel processing
k8s_executor = k8s_job_executor.configured(
    {
        "job_namespace": "dagster",
        "image_pull_policy": "Never",
        "service_account_name": "default",
        "max_concurrent": 3,  # Reduced from 5 to 3 to prevent memory overload
        "step_k8s_config": {
            "container_config": {
                "resources": {
                    "requests": {
                        "cpu": "250m",
                        "memory": "512Mi"
                    },
                    "limits": {
                        "cpu": "1",
                        "memory": "2Gi"
                    }
                }
            }
        }
    }
)

# Define a dedicated asset job with the K8s executor
ndvi_processing_job = define_asset_job(
    name="ndvi_processing_job",
    selection=[AssetKey("ndvi_by_field")],
    executor_def=k8s_executor,
    config={
        "execution": {
            "config": {
                "max_concurrent": 3
            }
        }
    },
    tags={
        "description": "Process NDVI data for fields from their respective planting dates",
        "output_structure": "output_data/{field_id}/{date}/ndvi_results.json and ndvi_plot.png",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "250m", "memory": "512Mi"},
                    "limits": {"cpu": "1", "memory": "2Gi"},
                }
            }
        }
    }
)

# Main Dagster definitions
definitions = Definitions(
    assets=[ndvi_by_field],
    sensors=[ndvi_input_sensor],
    jobs=[ndvi_processing_job],
    resources={
        # MinIO resource for S3-compatible storage
        "minio": MinioResource(
            endpoint_url=EnvVar("MINIO_ENDPOINT_URL"),
            aws_access_key_id=EnvVar("MINIO_ACCESS_KEY"),
            aws_secret_access_key=EnvVar("MINIO_SECRET_KEY"),
            bucket_name=EnvVar("MINIO_BUCKET_NAME"),
        ),
        
        # S3 resource for asset inputs/outputs
        "s3": S3Resource(
            endpoint_url=EnvVar("MINIO_ENDPOINT_URL"),
            aws_access_key_id=EnvVar("MINIO_ACCESS_KEY"),
            aws_secret_access_key=EnvVar("MINIO_SECRET_KEY"),
        ),
        
        # We don't use s3_io_manager directly - the assets will handle S3 I/O
        
        # Fallback local IO manager for testing
        "io_manager": local_io_manager,
    },
) 