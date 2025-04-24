# Placeholder for Dagster sensors 

from typing import List
from datetime import datetime, timedelta

from dagster import (
    RunRequest,
    sensor,
    SkipReason,
    SensorEvaluationContext,
    RunConfig,
    AssetSelection,
    DefaultSensorStatus,
    AssetKey
)

from dagster_ndvi_project.resources import MinioResource


@sensor(
    minimum_interval_seconds=10,
    required_resource_keys={"minio"},
    default_status=DefaultSensorStatus.RUNNING,
    job_name="ndvi_processing_job"
)
def ndvi_input_sensor(context: SensorEvaluationContext):
    """
    Sensor that detects new input files in the MinIO bucket and triggers a run to materialize the NDVI asset.
    Expects both bounding_box.geojson and fields.geojson files to be present.
    """
    # Get MinIO resource
    minio = context.resources.minio
    s3_client = minio.get_s3_client()
    bucket_name = minio.bucket_name
    
    # Check for input files
    input_prefix = "input_data/"
    
    # Get a list of files in the input directory
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=input_prefix,
            Delimiter="/"
        )
    except Exception as e:
        context.log.error(f"Error listing objects in bucket: {e}")
        return SkipReason(f"Error accessing MinIO: {str(e)}")
    
    # Check if both required files exist
    has_bbox = False
    has_fields = False
    
    # Filter files to find our GeoJSON inputs
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('bounding_box.geojson'):
            has_bbox = True
        elif key.endswith('fields.geojson'):
            has_fields = True
    
    if not (has_bbox and has_fields):
        missing = []
        if not has_bbox:
            missing.append("bounding_box.geojson")
        if not has_fields:
            missing.append("fields.geojson")
        return SkipReason(f"Missing required input files: {', '.join(missing)}")
    
    # Both files are present, use a valid partition key within the defined range
    # Using a fixed date within the partition definition range (which starts at 2023-01-01)
    partition_key = "2023-05-15"
    
    # Log the detected files
    context.log.info(f"Detected input files for NDVI calculation. Triggering run for partition {partition_key}.")
    
    # Simply return a run request with the appropriate asset selection and partition key
    return RunRequest(
        run_key=f"ndvi_{partition_key}",
        tags={
            "source": "input_sensor",
            "partition": partition_key
        },
        asset_selection=[AssetKey("ndvi_by_field")],
        partition_key=partition_key
    ) 