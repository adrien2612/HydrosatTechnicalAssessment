# Placeholder for Dagster sensors 

from typing import List
from datetime import datetime, timedelta
import json
import pandas as pd
import geopandas as gpd
from io import StringIO
import os
import boto3
import traceback
from shapely.geometry import shape

from dagster import (
    RunRequest,
    sensor,
    SkipReason,
    SensorEvaluationContext,
    RunConfig,
    AssetSelection,
    DefaultSensorStatus,
    AssetKey,
    AutoMaterializePolicy,
    AutoMaterializeRule
)

from dagster_ndvi_project.resources import MinioResource


@sensor(
    minimum_interval_seconds=60,  # Check every minute
    required_resource_keys={"minio"},
    default_status=DefaultSensorStatus.RUNNING,
    job_name="ndvi_processing_job"
)
def ndvi_input_sensor(context: SensorEvaluationContext):
    """
    Sensor that detects new input files in the MinIO bucket and triggers a backfill
    from each field's planting date to the current date.
    Expects both bounding_box.geojson and fields.geojson files to be present.
    For each field in fields.geojson, checks if it's within the bounding box.
    If a field is within the bounding box, triggers a backfill from that field's planting date to the current date.
    """
    try:
        # Get MinIO resource
        minio = context.resources.minio
        
        # Log all environment variables for debugging
        context.log.info(f"MinIO Endpoint URL from resource: {minio.endpoint_url}")
        context.log.info(f"MinIO Bucket Name from resource: {minio.bucket_name}")
        
        # Try multiple endpoint URLs if we're having connection issues
        endpoint_urls = [
            minio.endpoint_url,                  # Original endpoint
            "http://localhost:9000",             # Local host directly
            "http://minio-local:9000",           # Container name
            "http://host.docker.internal:9000",  # Docker host
        ]
        
        # Test connectivity with all endpoints
        s3_client = None
        for endpoint_url in endpoint_urls:
            try:
                context.log.info(f"Testing MinIO connectivity with endpoint: {endpoint_url}")
                s3_client = boto3.client(
                    "s3",
                    endpoint_url=endpoint_url,
                    aws_access_key_id=minio.aws_access_key_id,
                    aws_secret_access_key=minio.aws_secret_access_key,
                    config=boto3.session.Config(s3={"addressing_style": "path"}),
                    verify=False
                )
                
                # List buckets as a connectivity test
                buckets = s3_client.list_buckets()
                context.log.info(f"Connection successful to {endpoint_url}. Found buckets: {[b['Name'] for b in buckets.get('Buckets', [])]}")
                
                # If we found our bucket, use this endpoint
                if any(bucket['Name'] == minio.bucket_name for bucket in buckets.get('Buckets', [])):
                    context.log.info(f"Found our bucket {minio.bucket_name} with endpoint {endpoint_url}")
                    break
            except Exception as e:
                context.log.info(f"Failed to connect to {endpoint_url}: {str(e)}")
        
        if not s3_client:
            return SkipReason(f"Could not connect to MinIO with any of the configured endpoints")
        
        bucket_name = minio.bucket_name
        context.log.info(f"Using MinIO endpoint: {endpoint_url} and bucket: {bucket_name}")
        
        # Check for input files
        input_prefix = "input_data/"
        
        try:
            # List objects in the input directory
            context.log.info(f"Listing objects with prefix {input_prefix}")
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=input_prefix
            )
            
            context.log.info(f"List objects response keys: {list(response.keys())}")
            
            if 'Contents' not in response or not response.get('Contents'):
                context.log.info(f"No files found in {input_prefix}")
                return SkipReason(f"No files found in {input_prefix}")
                
            context.log.info(f"Found {len(response.get('Contents', []))} files in input directory")
            
        except Exception as e:
            context.log.error(f"Error listing objects in bucket: {e}")
            context.log.error(f"Traceback: {traceback.format_exc()}")
            return SkipReason(f"Error accessing MinIO: {str(e)}")
        
        # Check if both required files exist
        has_bbox = False
        has_fields = False
        bbox_key = None
        fields_key = None
        
        # Filter files to find our GeoJSON inputs
        for obj in response.get('Contents', []):
            key = obj['Key']
            context.log.info(f"Found file: {key}")
            if 'bounding_box' in key and key.endswith('.geojson'):
                bbox_key = key
                has_bbox = True
                context.log.info(f"Identified bounding box file: {bbox_key}")
            elif 'fields' in key and key.endswith('.geojson'):
                fields_key = key
                has_fields = True
                context.log.info(f"Identified fields file: {fields_key}")
        
        if not (has_bbox and has_fields):
            missing = []
            if not has_bbox:
                missing.append("bounding_box.geojson")
            if not has_fields:
                missing.append("fields.geojson")
            return SkipReason(f"Missing required input files: {', '.join(missing)}")
        
        # Extract last modified timestamp of fields.geojson
        fields_obj = next((obj for obj in response.get('Contents', []) if obj['Key'] == fields_key), None)
        
        if not fields_obj:
            return SkipReason("Could not get metadata for fields.geojson")
        
        # Get last processed timestamp from sensor state
        last_processed_timestamp = context.cursor or "0"
        fields_last_modified = fields_obj['LastModified'].isoformat()
        
        context.log.info(f"Last processed: {last_processed_timestamp}, Current modified: {fields_last_modified}")
        
        # Read the fields file to extract field IDs and planting dates
        try:
            # Get the bounding box GeoJSON from S3/MinIO
            context.log.info(f"Reading bounding box from {bbox_key}")
            bbox_response = s3_client.get_object(Bucket=bucket_name, Key=bbox_key)
            bbox_content = bbox_response['Body'].read().decode('utf-8')
            
            # Parse the bounding box GeoJSON
            bbox_data = json.loads(bbox_content)
            if 'features' in bbox_data and len(bbox_data['features']) > 0:
                # Extract the bounding box geometry
                bbox_geometry = shape(bbox_data['features'][0]['geometry'])
                context.log.info(f"Successfully parsed bounding box geometry")
            else:
                return SkipReason("Invalid bounding box GeoJSON format")
            
            # Get the fields GeoJSON from S3/MinIO
            context.log.info(f"Reading fields from {fields_key}")
            fields_response = s3_client.get_object(Bucket=bucket_name, Key=fields_key)
            fields_content = fields_response['Body'].read().decode('utf-8')
            
            context.log.info(f"Fields content size: {len(fields_content)} bytes")
            
            # Parse the GeoJSON file into a GeoDataFrame
            fields_gdf = gpd.read_file(StringIO(fields_content))
            
            context.log.info(f"Parsed GeoJSON with {len(fields_gdf)} fields")
            
            # Check for required fields
            if 'field_id' not in fields_gdf.columns or 'planting_date' not in fields_gdf.columns:
                return SkipReason("Fields GeoJSON is missing required columns (field_id and/or planting_date)")
            
            # Convert planting dates to datetime objects
            fields_gdf['planting_date'] = pd.to_datetime(fields_gdf['planting_date'])
            
            # Get the current date to use as the end date for backfill
            current_date = datetime.now()
            context.log.info(f"Current date for backfill end: {current_date.strftime('%Y-%m-%d')}")
            
            # Create run requests for each field from planting date to current date
            run_requests = []
            processed_field_count = 0
            
            # Process all fields
            for idx, field in fields_gdf.iterrows():
                field_id = field['field_id']
                field_geometry = field['geometry']
                
                # Check if the field is within the bounding box
                if not field_geometry.intersects(bbox_geometry):
                    context.log.info(f"Field {field_id} is not within the bounding box, skipping")
                    continue
                
                context.log.info(f"Field {field_id} is within the bounding box, processing")
                
                # Get the planting date for this field
                planting_date = field['planting_date'].to_pydatetime()
                
                # Ensure planting date is valid (not in the future)
                if planting_date > current_date:
                    context.log.info(f"Field {field_id} has future planting date {planting_date.date()}, skipping")
                    continue
                
                processed_field_count += 1
                context.log.info(f"Field {field_id}: Planting date {planting_date.strftime('%Y-%m-%d')}")
                
                # For testing purposes, limit to processing 5 days from planting date
                # In production, you'd use: date_range = pd.date_range(start=planting_date, end=current_date)
                max_days = 5  # Limit to 5 days for testing
                end_date = min(current_date, planting_date + timedelta(days=max_days))
                date_range = pd.date_range(start=planting_date, end=end_date)
                
                context.log.info(f"Field {field_id}: Processing {len(date_range)} days from {planting_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                
                # Create run requests for each date
                for process_date in date_range:
                    partition_key = process_date.strftime("%Y-%m-%d")
                    run_key = f"ndvi_{field_id}_{partition_key}"
                    
                    context.log.info(f"Creating run request for field {field_id} on date {partition_key}")
                    
                    run_requests.append(
                        RunRequest(
                            run_key=run_key,
                            tags={
                                "source": "input_sensor_backfill",
                                "partition": partition_key,
                                "field_id": field_id
                            },
                            asset_selection=[AssetKey("ndvi_by_field")],
                            partition_key=partition_key
                        )
                    )
            
            # Update the cursor with the current timestamp
            context.update_cursor(fields_last_modified)
            
            if not run_requests:
                return SkipReason("No valid fields within bounding box or with valid planting dates")
            
            # Return all run requests
            context.log.info(f"Processed {processed_field_count} fields with a total of {len(run_requests)} days of NDVI data")
            return run_requests
            
        except Exception as e:
            context.log.error(f"Error processing input files: {str(e)}")
            context.log.error(f"Traceback: {traceback.format_exc()}")
            return SkipReason(f"Error processing input files: {str(e)}")
    
    except Exception as e:
        context.log.error(f"Unexpected error in sensor: {str(e)}")
        context.log.error(f"Traceback: {traceback.format_exc()}")
        return SkipReason(f"Unexpected error in sensor: {str(e)}") 