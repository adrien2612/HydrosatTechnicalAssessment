# definitions.py

from dagster import Definitions, EnvVar, FilesystemIOManager, define_asset_job, AssetKey
from dagster_aws.s3 import S3Resource
from dagster_k8s import k8s_job_executor

# Use relative imports within the package
from dagster_ndvi_project.dagster_ndvi_project.resources import MinioResource
from dagster_ndvi_project.dagster_ndvi_project.assets import compute_ndvi_raw, load_fields
from dagster_ndvi_project.dagster_ndvi_project.sensors.optimized_ndvi_sensor import optimized_ndvi_sensor

# Local filesystem IO (for intermediate testing)
local_io_manager = FilesystemIOManager(base_dir="./data")

# Define the job without a specific executor
ndvi_processing_job = define_asset_job(
    name="ndvi_processing_job",
    selection=[AssetKey("compute_ndvi_raw"), AssetKey("load_fields")],
)

definitions = Definitions(
    assets=[compute_ndvi_raw, load_fields],
    sensors=[optimized_ndvi_sensor],
    jobs=[ndvi_processing_job],
    resources={
        "minio": MinioResource(
            endpoint_url=EnvVar("MINIO_ENDPOINT_URL"),
            aws_access_key_id=EnvVar("MINIO_ACCESS_KEY"),
            aws_secret_access_key=EnvVar("MINIO_SECRET_KEY"),
            bucket_name=EnvVar("MINIO_BUCKET_NAME"),
        ),
        "s3": S3Resource(
            endpoint_url=EnvVar("MINIO_ENDPOINT_URL"),
            aws_access_key_id=EnvVar("MINIO_ACCESS_KEY"),
            aws_secret_access_key=EnvVar("MINIO_SECRET_KEY"),
        ),
        "io_manager": local_io_manager,
    },
)
