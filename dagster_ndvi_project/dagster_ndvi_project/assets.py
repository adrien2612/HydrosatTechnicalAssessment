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
daily_partitions = DailyPartitionsDefinition(start_date="2023-01-01")


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
    """
    # Get partition date
    partition_date_str = context.partition_key
    context.log.info(f"Processing partition: {partition_date_str}")
    
    # Parse the partition date
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
    
    # Get MinIO resource
    minio = context.resources.minio
    s3_client = minio.get_s3_client()
    bucket_name = minio.bucket_name
    
    # Try to get previous day's data if available
    previous_ndvi_by_field = {}
    previous_date = partition_date - timedelta(days=1)
    previous_date_str = previous_date.strftime("%Y-%m-%d")
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
        
        # Write NDVI to the temporary file
        with rasterio.open(tmp_ndvi.name, 'w', **profile) as dst:
            dst.write(ndvi, 1)
        
        # For each valid field, calculate the mean NDVI
        for idx, field in valid_fields.iterrows():
            field_id = field['field_id']
            field_geom = field['geometry']
            
            # Use the field geometry to mask the NDVI raster
            with rasterio.open(tmp_ndvi.name) as src:
                out_image, out_transform = mask(src, [field_geom], crop=True)
            
            # Calculate mean NDVI for this field
            field_ndvi = float(np.nanmean(out_image))
            
            # Store the result
            ndvi_results[field_id] = field_ndvi
            context.log.info(f"Field {field_id}: Mean NDVI = {field_ndvi:.4f}")
    
    # Move the input files to the processed_data folder
    new_bbox_key = input_prefix + "processed_data/" + os.path.basename(bbox_key)
    new_fields_key = input_prefix + "processed_data/" + os.path.basename(fields_key)
    
    # Copy objects to processed_data folder
    s3_client.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': bucket_name, 'Key': bbox_key},
        Key=new_bbox_key
    )
    s3_client.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': bucket_name, 'Key': fields_key},
        Key=new_fields_key
    )
    
    # Delete original files after copying
    s3_client.delete_object(Bucket=bucket_name, Key=bbox_key)
    s3_client.delete_object(Bucket=bucket_name, Key=fields_key)
    
    context.log.info(f"Moved input files to processed_data folder.")
    
    # Create a visualization of the results
    if ndvi_results:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Extract field IDs and NDVI values for plotting
        field_ids = list(ndvi_results.keys())
        ndvi_values = list(ndvi_results.values())
        
        # Sort by field ID
        sorted_idx = np.argsort(field_ids)
        sorted_field_ids = [field_ids[i] for i in sorted_idx]
        sorted_ndvi_values = [ndvi_values[i] for i in sorted_idx]
        
        # Bar chart of NDVI values by field
        bars = ax.bar(sorted_field_ids, sorted_ndvi_values)
        
        # Add previous day's values as reference points if available
        if previous_ndvi_by_field:
            prev_values = []
            for field_id in sorted_field_ids:
                if field_id in previous_ndvi_by_field:
                    prev_values.append(previous_ndvi_by_field[field_id])
                else:
                    prev_values.append(None)
            
            # Only add scatter points where we have values
            valid_indexes = [i for i, v in enumerate(prev_values) if v is not None]
            if valid_indexes:
                valid_fields = [sorted_field_ids[i] for i in valid_indexes]
                valid_prev_values = [prev_values[i] for i in valid_indexes]
                ax.scatter(valid_fields, valid_prev_values, color='red', marker='o', label=f'Previous ({(partition_date - timedelta(days=1)).strftime("%Y-%m-%d")})')
                ax.legend()
        
        # Formatting
        ax.set_xlabel('Field ID')
        ax.set_ylabel('NDVI')
        ax.set_title(f'Mean NDVI by Field - {partition_date_str}')
        ax.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save the plot to a BytesIO object
        plot_buffer = io.BytesIO()
        plt.savefig(plot_buffer, format='png')
        plot_buffer.seek(0)
        
        # Upload the visualization to the output folder
        viz_key = f"output_data/{partition_date_str}/ndvi_visualization.png"
        s3_client.upload_fileobj(
            plot_buffer, 
            bucket_name, 
            viz_key,
            ExtraArgs={'ContentType': 'image/png'}
        )
        
        context.log.info(f"Uploaded visualization to {viz_key}")
        plt.close()
    
    # Save the NDVI results to S3 for future reference
    results_key = f"output_data/{partition_date_str}/ndvi_results.json"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=results_key,
        Body=json.dumps(ndvi_results),
        ContentType='application/json'
    )
    
    context.log.info(f"Saved NDVI results to {results_key}")
    
    # Log metadata about the result
    context.add_output_metadata({
        "num_fields": len(ndvi_results),
        "field_ids": MetadataValue.json(list(ndvi_results.keys())),
        "min_ndvi": min(ndvi_results.values()) if ndvi_results else None,
        "max_ndvi": max(ndvi_results.values()) if ndvi_results else None,
        "date": partition_date_str,
    })
    
    # Return the results
    return ndvi_results 