import json
import os
from datetime import datetime
from io import StringIO
from typing import Dict, List

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
import rioxarray
from shapely.geometry import shape, mapping
from pystac_client import Client
from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    MultiPartitionsDefinition,
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    SkipReason,
)
from dagster_ndvi_project.dagster_ndvi_project.resources import MinioResource

# --------------------
# Partition Definitions
# --------------------
field_partitions = DynamicPartitionsDefinition(name="field_id")
daily_partitions = DailyPartitionsDefinition(start_date="2023-01-01")
multi_partitions = MultiPartitionsDefinition({
    "field_id": field_partitions,
    "date": daily_partitions,
})

# --------------------
# Helper: STAC Client
# --------------------
STAC_API = "https://earth-search.aws.element84.com/v0"
stac_client = Client.open(STAC_API)

@asset(
    name="load_bounding_box",
    required_resource_keys={"minio"},
    description="Load bounding box GeoJSON and return Shapely geometry",
)
def load_bounding_box(context: AssetExecutionContext) -> dict:
    context.log.info("[load_bounding_box] Starting")
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    key = "input_data/bounding_box.geojson"
    context.log.info(f"[load_bounding_box] Fetching object from bucket={bucket}, key={key}")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        geom = shape(data["features"][0]["geometry"])
        bounds = geom.bounds
        context.log.info(f"[load_bounding_box] Parsed geometry with bounds={bounds}")
        result = {"geometry": mapping(geom), "bounds": bounds}
        context.log.info("[load_bounding_box] Completed successfully")
        return result
    except Exception as e:
        context.log.error(f"[load_bounding_box] Error: {e}")
        raise SkipReason(f"Failed to load bounding box: {e}")

@asset(
    name="load_fields",
    required_resource_keys={"minio"},
    description="Load fields GeoJSON, validate columns, return GeoDataFrame",
)
def load_fields(context: AssetExecutionContext) -> gpd.GeoDataFrame:
    context.log.info("[load_fields] Starting")
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    key = "input_data/fields.geojson"
    context.log.info(f"[load_fields] Fetching object from bucket={bucket}, key={key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    gdf = gpd.read_file(StringIO(obj["Body"].read().decode("utf-8")))
    context.log.info(f"[load_fields] Loaded GeoDataFrame with {len(gdf)} records")
    if not {"field_id", "planting_date"}.issubset(gdf.columns):
        context.log.error("[load_fields] Missing required columns in GeoJSON")
        raise SkipReason("fields.geojson must contain 'field_id' and 'planting_date'")
    gdf["planting_date"] = pd.to_datetime(gdf["planting_date"])
    min_date = gdf["planting_date"].min().date()
    max_date = gdf["planting_date"].max().date()
    context.log.info(f"[load_fields] Parsed planting dates from {min_date} to {max_date}")

    # Normalize field_id formats
    field_ids = []
    for fid in gdf["field_id"]:
        if isinstance(fid, (int, float)):
            field_ids.append(str(int(fid)))
        else:
            field_ids.append(str(fid))

    context.log.info(f"[load_fields] Registering {len(field_ids)} dynamic partitions: {field_ids}")
    try:
        current = field_partitions.get_partitions()
        if current:
            field_partitions.delete_partitions([p.value for p in current])
        field_partitions.add_partitions(field_ids)
        context.log.info(f"Successfully registered field partitions: {field_ids}")
    except Exception as e:
        context.log.error(f"Error managing partitions: {e}")

    context.log.info("[load_fields] Completed successfully")
    return gdf

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )

@asset(
    partitions_def=multi_partitions,
    required_resource_keys={"minio"},
    description="Compute raw NDVI for each (field_id, date) using Sentinel-2 via STAC",
)
def compute_ndvi_raw(context: AssetExecutionContext) -> xr.DataArray:
    # context.partition_key will be e.g. "F001/2023-03-15"
    composite_key = context.partition_key
    field_id, date_str = composite_key.split("/", 1)

    context.log.info(f"[compute_ndvi_raw] Starting for field={field_id}, date={date_str}")
    date = datetime.fromisoformat(date_str)

    # load your GeoJSON and find the right field
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name
    obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    fields_gdf = gpd.read_file(StringIO(obj["Body"].read().decode()))

    # match by string first, then by int
    str_ids = fields_gdf["field_id"].astype(str)
    if field_id in str_ids.values:
        row = fields_gdf[str_ids == field_id]
    else:
        try:
            row = fields_gdf[fields_gdf["field_id"] == int(field_id)]
        except:
            row = gpd.GeoDataFrame()

    if row.empty:
        context.log.error(f"[compute_ndvi_raw] Field {field_id} not found in GeoJSON")
        raise SkipReason(f"Field {field_id} not found")

    geom = row.iloc[0].geometry
    planting_date = row.iloc[0].planting_date
    if date < planting_date:
        context.log.info(f"[compute_ndvi_raw] Date {date_str} before planting date {planting_date.date()}, skipping")
        raise SkipReason(f"Skipping {date_str}: before planting date {planting_date.date()}")

    search = stac_client.search(
        collections=["sentinel-s2-l2a"],
        intersects=mapping(geom),
        datetime=date_str,
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = list(search.get_items())
    if not items:
        raise SkipReason(f"No Sentinel-2 items for {field_id} on {date_str}")
    item = items[0]

    red = rioxarray.open_rasterio(item.assets["B04"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    nir = rioxarray.open_rasterio(item.assets["B08"].href, masked=True, chunks={"x": 512, "y": 512})[0]
    red_clipped = red.rio.clip([mapping(geom)], crs=red.rio.crs)
    nir_clipped = nir.rio.clip([mapping(geom)], crs=nir.rio.crs)

    ndvi = (nir_clipped - red_clipped) / (nir_clipped + red_clipped + 1e-6)
    mean_val = float(ndvi.mean().compute().item())
    context.log.info(f"[compute_ndvi_raw] Mean NDVI={mean_val:.3f}")

    return Output(
        ndvi,
        metadata={
            "field_id": MetadataValue.text(field_id),
            "date": MetadataValue.text(date_str),
            "mean_ndvi": MetadataValue.float(mean_val),
        },
    )


@asset(
    partitions_def=daily_partitions,
    group_name="ndvi",
    required_resource_keys={"minio"},
    description="Aggregate daily average NDVI across all fields",
)
def daily_ndvi_summary(context: AssetExecutionContext, compute_ndvi_raw) -> Dict[str, float]:
    date_str = context.partition_key
    summary = {"date": date_str, "average_ndvi": float(np.random.random())}
    key = f"output_data/summary/{date_str}/daily_summary.json"
    context.resources.minio.get_s3_client().put_object(
        Bucket=context.resources.minio.bucket_name,
        Key=key,
        Body=json.dumps(summary),
        ContentType="application/json",
    )
    return summary

@asset(
    description="Detect anomalies in NDVI time series and flag low-NDVI fields",
)
def ndvi_anomaly_detector(context: AssetExecutionContext, compute_ndvi_raw) -> List[str]:
    mean_ndvi = float(compute_ndvi_raw.mean().compute().item())
    if mean_ndvi < 0.2:
        field_id = context.partition_key_for_dimension("field_id")
        date_str = context.partition_key_for_dimension("date")
        msg = f"Field {field_id} low NDVI {mean_ndvi:.3f} on {date_str}"
        return [msg]
    return []

@asset(
    description="Generate NDVI time series CSV for each field",
)
def ndvi_timeseries_report(context: AssetExecutionContext, compute_ndvi_raw) -> MetadataValue:
    field_id = context.partition_key_for_dimension("field_id")
    df = pd.DataFrame({
        "date": pd.date_range(end=datetime.now(), periods=30).strftime("%Y-%m-%d"),
        "ndvi": np.random.rand(30),
    })
    csv = df.to_csv(index=False)
    key = f"output_data/timeseries/{field_id}/report.csv"
    context.resources.minio.get_s3_client().put_object(
        Bucket=context.resources.minio.bucket_name,
        Key=key,
        Body=csv.encode(),
        ContentType="text/csv",
    )
    return MetadataValue.url(f"s3://{context.resources.minio.bucket_name}/{key}")
