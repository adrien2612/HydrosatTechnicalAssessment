# Placeholder for Dagster assets 

import os
from datetime import datetime, timedelta
import json
import tempfile
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.mask import mask
from shapely.geometry import shape, Polygon, box
from pystac_client import Client
import planetary_computer
import dateutil.parser
from dagster import asset, AssetExecutionContext, DailyPartitionsDefinition, Output, MetadataValue
import boto3
import matplotlib.pyplot as plt
import io
from io import StringIO

from dagster_ndvi_project.resources import MinioResource


# Function to download Sentinel-2 data for a given date and bounding box
def download_sentinel2_data(bbox: Polygon, date: datetime) -> Tuple[np.ndarray, np.ndarray]:
    """
    Download Sentinel-2 data for a given date and bounding box.
    Returns red and NIR bands.
    """
    # Convert bbox to a list of coordinates for STAC API
    bbox_coords = list(bbox.bounds)  # minx, miny, maxx, maxy
    
    # Format date strings for STAC query
    date_str = date.strftime("%Y-%m-%d")
    next_date_str = (date + timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Connect to Planetary Computer STAC API
    catalog = Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    
    # Search for Sentinel-2 L2A scenes
    search = catalog.search(
        collections=["sentinel-2-l2a"],
        bbox=bbox_coords,
        datetime=f"{date_str}/{next_date_str}",
        query={"eo:cloud_cover": {"lt": 20}},  # Filter for low cloud cover
    )
    
    # Get items (scenes) from search results
    items = list(search.get_items())
    
    if not items:
        print(f"No Sentinel-2 data found for {date_str} in the given bounding box.")
        # Return empty arrays
        return np.array([]), np.array([])
    
    # Select the first item with the lowest cloud cover
    items.sort(key=lambda x: x.properties.get("eo:cloud_cover", 100))
    scene = items[0]
    
    # Get red (B04) and near-infrared (B08) bands
    red_href = scene.assets["red"].href
    nir_href = scene.assets["nir"].href
    
    # Download and read the bands using rasterio
    with rasterio.open(red_href) as red_src:
        red_data = red_src.read(1)
        transform = red_src.transform
        crs = red_src.crs
    
    with rasterio.open(nir_href) as nir_src:
        nir_data = nir_src.read(1)
    
    return red_data, nir_data


# Function to calculate NDVI from red and NIR bands
def calculate_ndvi(red_band: np.ndarray, nir_band: np.ndarray) -> np.ndarray:
    """Calculate NDVI from red and NIR bands."""
    # Handle empty inputs
    if red_band.size == 0 or nir_band.size == 0:
        return np.array([])
    
    # Avoid division by zero by adding a small epsilon
    epsilon = 1e-10
    
    # Convert to float32 for computation to avoid integer division issues
    red_band = red_band.astype(np.float32)
    nir_band = nir_band.astype(np.float32)
    
    # Apply scaling factor if needed (depends on Sentinel-2 L2A format)
    # For surface reflectance (L2A), values are typically 0-10000
    # Scale to 0-1 if needed
    if red_band.max() > 1.0:
        red_band = red_band / 10000.0
    if nir_band.max() > 1.0:
        nir_band = nir_band / 10000.0
    
    # Calculate NDVI: (NIR - Red) / (NIR + Red)
    ndvi = (nir_band - red_band) / (nir_band + red_band + epsilon)
    
    # Clip to valid NDVI range [-1, 1]
    ndvi = np.clip(ndvi, -1.0, 1.0)
    
    return ndvi


# Define the daily partitions
daily_partitions = DailyPartitionsDefinition(start_date="2023-01-01", end_date="2025-04-24")


@asset(
    partitions_def=daily_partitions,
    group_name="ndvi",
    required_resource_keys={"minio"},
)
def ndvi_by_field(context: AssetExecutionContext) -> Dict:
    """
    Asset that calculates NDVI for each field within a bounding box.
    Requires two input GeoJSON files in the MinIO bucket: bounding_box.geojson and fields.geojson.
    The output is a dictionary with field IDs as keys and NDVI values as values.
    When run with a field_id tag, processes only that specific field.
    """
    # Get partition date
    partition_date_str = context.partition_key
    context.log.info(f"Processing partition: {partition_date_str}")
    
    # Parse the partition date
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
    
    # Check if we're processing a specific field
    specific_field_id = context.get_tag("field_id")
    if specific_field_id:
        context.log.info(f"Processing specific field: {specific_field_id}")
    else:
        context.log.info("Processing all fields")
    
    # Get MinIO resource
    minio = context.resources.minio
    s3_client = minio.get_s3_client()
    bucket_name = minio.bucket_name
    
    # Try to get previous day's data if available
    previous_ndvi_by_field = {}
    previous_date = partition_date - timedelta(days=1)
    previous_date_str = previous_date.strftime("%Y-%m-%d")
    
    if specific_field_id:
        previous_data_key = f"output_data/{specific_field_id}/{previous_date_str}/ndvi_results.json"
    else:
        previous_data_key = f"output_data/{previous_date_str}/ndvi_results.json"
    
    try:
        previous_data_response = s3_client.get_object(Bucket=bucket_name, Key=previous_data_key)
        previous_data_content = previous_data_response['Body'].read().decode('utf-8')
        previous_ndvi_by_field = json.loads(previous_data_content)
        context.log.info(f"Loaded previous NDVI data from {previous_date_str}")
    except Exception as e:
        context.log.info(f"No previous NDVI data available: {str(e)}")
    
    # Check for input files
    input_prefix = "input_data/"
    
    # Get a list of files in the input directory
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=input_prefix,
        Delimiter="/"
    )
    
    # Check if both required files exist
    has_bbox = False
    has_fields = False
    
    # Filter files to find our GeoJSON inputs
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('bounding_box.geojson'):
            bbox_key = key
            has_bbox = True
        elif key.endswith('fields.geojson'):
            fields_key = key
            has_fields = True
    
    if not (has_bbox and has_fields):
        context.log.error("Missing required input GeoJSON files.")
        # Return empty result
        return {}
    
    # Get the data from S3/MinIO
    context.log.info(f"Reading bounding box from {bbox_key}")
    bbox_response = s3_client.get_object(Bucket=bucket_name, Key=bbox_key)
    bbox_content = bbox_response['Body'].read().decode('utf-8')
    
    context.log.info(f"Reading fields from {fields_key}")
    fields_response = s3_client.get_object(Bucket=bucket_name, Key=fields_key)
    fields_content = fields_response['Body'].read().decode('utf-8')
    
    # Parse the GeoJSON files
    bbox_geojson = json.loads(bbox_content)
    fields_geojson = json.loads(fields_content)
    
    # Extract the bounding box geometry as a shapely object
    bbox_geom = shape(bbox_geojson['features'][0]['geometry'])
    
    # Create a GeoDataFrame from the fields GeoJSON
    fields_gdf = gpd.read_file(StringIO(fields_content))
    
    # Check for required fields in the fields GeoJSON
    if 'field_id' not in fields_gdf.columns:
        context.log.error("'field_id' is missing from fields GeoJSON.")
        return {}
    
    if 'planting_date' not in fields_gdf.columns:
        context.log.error("'planting_date' is missing from fields GeoJSON.")
        return {}
    
    # Convert planting date strings to datetime
    fields_gdf['planting_date'] = pd.to_datetime(fields_gdf['planting_date'])
    
    # Filter for specific field if specified
    if specific_field_id:
        fields_gdf = fields_gdf[fields_gdf['field_id'] == specific_field_id]
        if fields_gdf.empty:
            context.log.error(f"Field ID {specific_field_id} not found in fields GeoJSON.")
            return {}
    
    # Get all valid fields (fields whose planting date is on or before the partition date)
    valid_fields = fields_gdf[fields_gdf['planting_date'] <= pd.to_datetime(partition_date_str)]
    
    if valid_fields.empty:
        context.log.info(f"No fields found with planting date on or before {partition_date_str}")
        return {}
    
    # Download Sentinel-2 data for the partition date and bounding box
    context.log.info(f"Downloading Sentinel-2 data for {partition_date_str}")
    red_band, nir_band = download_sentinel2_data(bbox_geom, partition_date)
    
    if red_band.size == 0 or nir_band.size == 0:
        context.log.warning(f"No Sentinel-2 data available for {partition_date_str}")
        # Return empty dict
        return {}
    
    # Calculate NDVI
    context.log.info(f"Calculating NDVI for {partition_date_str}")
    ndvi = calculate_ndvi(red_band, nir_band)
    
    # Store NDVI values for each field
    ndvi_results = {}
    
    # Process NDVI for each field
    for idx, field in valid_fields.iterrows():
        field_id = field['field_id']
        field_geom = field.geometry
        
        # Create a temporary NDVI file to use with rasterio
        with tempfile.NamedTemporaryFile(suffix='.tif') as tmp_ndvi:
            # Create a basic profile from red band (assuming the same dimensions)
            profile = {
                'driver': 'GTiff',
                'height': ndvi.shape[0],
                'width': ndvi.shape[1],
                'count': 1,
                'dtype': ndvi.dtype,
                'crs': '+proj=utm +zone=33 +datum=WGS84 +units=m +no_defs',  # Example CRS, adjust as needed
                'transform': rasterio.transform.from_bounds(
                    *bbox_geom.bounds, ndvi.shape[1], ndvi.shape[0]
                )
            }
            
            # Write NDVI to temporary file
            with rasterio.open(tmp_ndvi.name, 'w', **profile) as dst:
                dst.write(ndvi, 1)
            
            # Read the file back to mask with the field geometry
            with rasterio.open(tmp_ndvi.name) as src:
                try:
                    # Convert field geometry to the same CRS as the NDVI raster if needed
                    field_geom_mask = [field_geom.__geo_interface__]
                    # Mask the NDVI raster with the field geometry
                    field_ndvi, _ = mask(src, field_geom_mask, crop=True, all_touched=True)
                    
                    # Calculate field statistics
                    field_ndvi_data = field_ndvi[0]
                    field_ndvi_mean = float(np.nanmean(field_ndvi_data))
                    field_ndvi_min = float(np.nanmin(field_ndvi_data))
                    field_ndvi_max = float(np.nanmax(field_ndvi_data))
                    
                    # Store in results
                    ndvi_results[field_id] = {
                        'date': partition_date_str,
                        'mean_ndvi': field_ndvi_mean,
                        'min_ndvi': field_ndvi_min,
                        'max_ndvi': field_ndvi_max,
                        'crop_type': field['crop_type'] if 'crop_type' in field else 'unknown',
                        'planting_date': field['planting_date'].strftime('%Y-%m-%d')
                    }
                    
                    # Generate a simple NDVI plot
                    plt.figure(figsize=(8, 6))
                    plt.imshow(field_ndvi[0], cmap='RdYlGn', vmin=-1, vmax=1)
                    plt.colorbar(label='NDVI')
                    plt.title(f"NDVI for Field {field_id} on {partition_date_str}")
                    
                    # Save the plot to a BytesIO object
                    plot_buffer = io.BytesIO()
                    plt.savefig(plot_buffer, format='png')
                    plot_buffer.seek(0)
                    plt.close()
                    
                    # Save to field-specific S3 directory
                    field_dir = f"output_data/{field_id}/{partition_date_str}"
                    
                    # Upload plot to S3
                    plot_key = f"{field_dir}/ndvi_plot.png"
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=plot_key,
                        Body=plot_buffer,
                        ContentType='image/png'
                    )
                    
                    # Save per-field result as JSON
                    field_result_key = f"{field_dir}/ndvi_results.json"
                    field_result = {field_id: ndvi_results[field_id]}
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=field_result_key,
                        Body=json.dumps(field_result, indent=2),
                        ContentType='application/json'
                    )
                    
                    context.log.info(f"Processed field {field_id} with mean NDVI: {field_ndvi_mean:.4f}")
                    
                except Exception as e:
                    context.log.error(f"Error processing field {field_id}: {str(e)}")
                    # Add error information to results
                    ndvi_results[field_id] = {
                        'date': partition_date_str,
                        'error': str(e)
                    }
    
    # Save combined results to S3/MinIO
    if specific_field_id:
        output_key = f"output_data/{specific_field_id}/{partition_date_str}/ndvi_results.json"
    else:
        output_key = f"output_data/{partition_date_str}/ndvi_results.json"
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=output_key,
        Body=json.dumps(ndvi_results, indent=2),
        ContentType='application/json'
    )
    
    # Add metadata to asset
    metadata = {
        "date": MetadataValue.text(partition_date_str),
        "num_fields_processed": MetadataValue.int(len(ndvi_results)),
        "fields": MetadataValue.json([f for f in ndvi_results.keys()]),
    }
    
    if specific_field_id:
        metadata["field_id"] = MetadataValue.text(specific_field_id)
    
    return Output(
        value=ndvi_results,
        metadata=metadata
    ) 