# dagster_ndvi_project/sensors/optimized_ndvi_sensor.py

from dagster import sensor, SensorEvaluationContext, RunRequest, SkipReason, AssetKey, DefaultSensorStatus, AssetSelection
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from io import StringIO
import hashlib
import json

@sensor(
    minimum_interval_seconds=60,
    required_resource_keys={"minio"},
    default_status=DefaultSensorStatus.RUNNING,  # Start the sensor automatically
    job_name="ndvi_processing_job"  # Specify the job to trigger
)
def optimized_ndvi_sensor(context: SensorEvaluationContext):
    """
    Sensor to trigger the load_fields asset when new data is detected.
    This will register field_id partitions and establish the foundation
    for downstream assets to be materialized.
    """
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name

    # list inputs
    objs = s3.list_objects_v2(Bucket=bucket, Prefix="input_data/").get("Contents", [])
    keys = {o["Key"] for o in objs}
    if not {"input_data/bounding_box.geojson", "input_data/fields.geojson"}.issubset(keys):
        return SkipReason("waiting for both bounding_box.geojson and fields.geojson")
    
    # Check for file modifications using etags and last_modified
    bounding_box_meta = s3.head_object(Bucket=bucket, Key="input_data/bounding_box.geojson")
    fields_meta = s3.head_object(Bucket=bucket, Key="input_data/fields.geojson")
    
    # Generate a combined hash of the ETags and last modified times
    current_hash = hashlib.md5(
        (bounding_box_meta["ETag"] + str(bounding_box_meta["LastModified"]) +
         fields_meta["ETag"] + str(fields_meta["LastModified"])).encode()
    ).hexdigest()
    
    # Check if we've seen these files before
    cursor = context.cursor or "{}"
    cursor_data = json.loads(cursor)
    last_hash = cursor_data.get("files_hash", "")
    
    # If no change in files, skip processing
    if current_hash == last_hash:
        return SkipReason("No new file uploads detected")
    
    # Update cursor with the new hash
    cursor_data["files_hash"] = current_hash
    context.update_cursor(json.dumps(cursor_data))
    
    # New or modified files detected, log info and trigger a run
    context.log.info("Detected new or modified input files.")
    context.log.info(f"Files detected: bounding_box.geojson, fields.geojson at {datetime.now().isoformat()}")
    
    # Return a RunRequest to trigger the job instead of skipping
    return RunRequest(run_key=f"input_files_{current_hash[:8]}", tags={"source": "optimized_ndvi_sensor"})
