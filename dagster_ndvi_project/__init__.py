# Main package __init__.py
from dagster import Definitions, EnvVar, FilesystemIOManager, define_asset_job, AssetKey
from dagster_aws.s3 import S3Resource

from dagster_ndvi_project.dagster_ndvi_project.resources import MinioResource
from dagster_ndvi_project.dagster_ndvi_project.assets import (
    load_bounding_box,
    load_fields,
    compute_ndvi_raw,
    daily_ndvi_summary,
    ndvi_anomaly_detector,
    ndvi_timeseries_report,
)
from dagster_ndvi_project.dagster_ndvi_project.sensors.optimized_ndvi_sensor import optimized_ndvi_sensor

# Local filesystem IO (for intermediate testing)
local_io_manager = FilesystemIOManager(base_dir="./data")

# Define the job without a specific executor
ndvi_processing_job = define_asset_job(
    name="ndvi_processing_job",
    selection=[AssetKey("compute_ndvi_raw")],
)

# Only define one Definitions object called 'defs'
defs = Definitions(
    assets=[
        load_bounding_box,
        load_fields,
        compute_ndvi_raw,
        daily_ndvi_summary,
        ndvi_anomaly_detector,
        ndvi_timeseries_report,
    ],
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