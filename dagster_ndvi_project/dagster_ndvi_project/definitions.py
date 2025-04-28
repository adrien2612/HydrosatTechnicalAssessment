# definitions.py

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetKey,
)
from .resources.minio_connection import MinioConnection
from .resources.s3_io_manager import S3IOManager
from .assets.upload_sentinel_images import all_sentinel_asset
from .assets.compute_ndvi import compute_ndvi_raw
from .assets.load_fields import load_fields
from .sensors.optimized_ndvi_sensor import optimized_ndvi_sensor

# Define a simple asset job without a specific executor
ndvi_processing_job = define_asset_job(
    name="ndvi_processing_job",
    selection=[AssetKey("compute_ndvi_raw"), AssetKey("load_fields")],
)

defs = Definitions(
    assets=[all_sentinel_asset, compute_ndvi_raw, load_fields],
    resources={
        "minio": MinioConnection(),
        "io_manager": S3IOManager()
    },
    sensors=[optimized_ndvi_sensor],
    jobs=[ndvi_processing_job],
)
