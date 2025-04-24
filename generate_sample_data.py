#!/usr/bin/env python3
"""
Script to generate sample GeoJSON files for testing the NDVI calculation asset.
This creates:
1. bounding_box.geojson - A rectangular area in an agricultural region
2. fields.geojson - Several polygon fields within the bounding box
"""

import json
import random
from datetime import datetime, timedelta
import argparse
import os
import boto3
from botocore.exceptions import NoCredentialsError


def generate_bbox_geojson(center_lat, center_lon, width_km, height_km):
    """Generate a bounding box GeoJSON around the given center point."""
    # Approximate degrees per km
    lat_deg_per_km = 1 / 110.574
    lon_deg_per_km = 1 / (111.320 * abs(center_lat))
    
    # Calculate coordinates
    min_lat = center_lat - (height_km / 2) * lat_deg_per_km
    max_lat = center_lat + (height_km / 2) * lat_deg_per_km
    min_lon = center_lon - (width_km / 2) * lon_deg_per_km
    max_lon = center_lon + (width_km / 2) * lon_deg_per_km
    
    # Create GeoJSON
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "name": "Processing Bounding Box",
                    "description": "Area of interest for satellite data processing"
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [min_lon, min_lat],
                        [max_lon, min_lat],
                        [max_lon, max_lat],
                        [min_lon, max_lat],
                        [min_lon, min_lat]
                    ]]
                }
            }
        ]
    }
    
    return geojson


def generate_random_field(center_lat, center_lon, max_radius_km):
    """Generate a random field polygon around the given center point."""
    # Approximate degrees per km
    lat_deg_per_km = 1 / 110.574
    lon_deg_per_km = 1 / (111.320 * abs(center_lat))
    
    # Random radius between 0.1 and max_radius_km
    radius_km = random.uniform(0.1, max_radius_km)
    
    # Generate random vertices for an irregular polygon
    num_vertices = random.randint(5, 10)
    vertices = []
    
    for i in range(num_vertices):
        angle = 2 * 3.14159 * i / num_vertices
        distance = radius_km * random.uniform(0.7, 1.0)
        
        lat_offset = distance * lat_deg_per_km * random.uniform(0.8, 1.2) * -1 if angle > 3.14159 else 1
        lon_offset = distance * lon_deg_per_km * random.uniform(0.8, 1.2) * -1 if 1.57 < angle < 4.71 else 1
        
        lat = center_lat + lat_offset
        lon = center_lon + lon_offset
        
        vertices.append([lon, lat])
    
    # Close the polygon
    vertices.append(vertices[0])
    
    return vertices


def generate_fields_geojson(center_lat, center_lon, num_fields, max_radius_km, planting_window_days):
    """Generate GeoJSON with multiple agricultural fields."""
    
    # Current date for reference
    now = datetime.now()
    
    # Create GeoJSON
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    # Generate random fields
    for i in range(num_fields):
        # Random center point near the main center
        field_center_lat = center_lat + random.uniform(-0.05, 0.05)
        field_center_lon = center_lon + random.uniform(-0.05, 0.05)
        
        # Random planting date within the planting window
        planting_date = (now - timedelta(days=random.randint(0, planting_window_days))).strftime("%Y-%m-%d")
        
        # Generate field coordinates
        field_coords = generate_random_field(field_center_lat, field_center_lon, max_radius_km)
        
        # Create feature
        feature = {
            "type": "Feature",
            "properties": {
                "field_id": f"field_{i+1}",
                "planting_date": planting_date,
                "crop_type": random.choice(["corn", "soybeans", "wheat", "cotton"])
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [field_coords]
            }
        }
        
        geojson["features"].append(feature)
    
    return geojson


def upload_to_minio(file_path, bucket, object_name, endpoint_url, access_key, secret_key):
    """Upload a file to MinIO."""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto3.session.Config(s3={"addressing_style": "path"}),
            verify=False if "localhost" in endpoint_url or "127.0.0.1" in endpoint_url else True,
        )
        
        s3_client.upload_file(file_path, bucket, object_name)
        return True
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except Exception as e:
        print(f"Upload error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Generate sample GeoJSON files for NDVI processing.')
    parser.add_argument('--center-lat', type=float, default=39.8283, 
                        help='Center latitude (default: 39.8283, Kansas)')
    parser.add_argument('--center-lon', type=float, default=-98.5795, 
                        help='Center longitude (default: -98.5795, Kansas)')
    parser.add_argument('--bbox-width', type=float, default=5.0, 
                        help='Bounding box width in km (default: 5.0)')
    parser.add_argument('--bbox-height', type=float, default=5.0, 
                        help='Bounding box height in km (default: 5.0)')
    parser.add_argument('--num-fields', type=int, default=5, 
                        help='Number of random fields to generate (default: 5)')
    parser.add_argument('--max-field-radius', type=float, default=1.0, 
                        help='Maximum field radius in km (default: 1.0)')
    parser.add_argument('--planting-window', type=int, default=180, 
                        help='Planting window in days (default: 180)')
    parser.add_argument('--upload', action='store_true', 
                        help='Upload to MinIO after generation')
    parser.add_argument('--minio-endpoint', type=str, default='http://localhost:9000', 
                        help='MinIO endpoint URL (default: http://localhost:9000)')
    parser.add_argument('--minio-access-key', type=str, default='minioadmin', 
                        help='MinIO access key (default: minioadmin)')
    parser.add_argument('--minio-secret-key', type=str, default='minioadmin', 
                        help='MinIO secret key (default: minioadmin)')
    parser.add_argument('--minio-bucket', type=str, default='dagster-data', 
                        help='MinIO bucket name (default: dagster-data)')
    
    args = parser.parse_args()
    
    # Generate GeoJSON files
    bbox_geojson = generate_bbox_geojson(args.center_lat, args.center_lon, args.bbox_width, args.bbox_height)
    fields_geojson = generate_fields_geojson(args.center_lat, args.center_lon, args.num_fields, 
                                           args.max_field_radius, args.planting_window)
    
    # Ensure output directory exists
    os.makedirs('sample_data', exist_ok=True)
    
    # Save files
    bbox_file = 'sample_data/bounding_box.geojson'
    fields_file = 'sample_data/fields.geojson'
    
    with open(bbox_file, 'w') as f:
        json.dump(bbox_geojson, f, indent=2)
    
    with open(fields_file, 'w') as f:
        json.dump(fields_geojson, f, indent=2)
    
    print(f"Generated sample data files:")
    print(f"  - {bbox_file}")
    print(f"  - {fields_file}")
    
    # Upload to MinIO if requested
    if args.upload:
        print("Uploading files to MinIO...")
        
        if upload_to_minio(bbox_file, args.minio_bucket, 'input_data/bounding_box.geojson', 
                          args.minio_endpoint, args.minio_access_key, args.minio_secret_key):
            print(f"  - Uploaded {bbox_file} to s3://{args.minio_bucket}/input_data/bounding_box.geojson")
        else:
            print(f"  - Failed to upload {bbox_file}")
        
        if upload_to_minio(fields_file, args.minio_bucket, 'input_data/fields.geojson', 
                          args.minio_endpoint, args.minio_access_key, args.minio_secret_key):
            print(f"  - Uploaded {fields_file} to s3://{args.minio_bucket}/input_data/fields.geojson")
        else:
            print(f"  - Failed to upload {fields_file}")


if __name__ == "__main__":
    main() 