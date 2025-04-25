# dagster_ndvi_project/sensors/optimized_ndvi_sensor.py

from dagster import sensor, SensorEvaluationContext, RunRequest, SkipReason
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from io import StringIO

@sensor(minimum_interval_seconds=60, required_resource_keys={"minio"})
def optimized_ndvi_sensor(context: SensorEvaluationContext):
    """
    On every run, look for both bounding_box.geojson & fields.geojson in MinIO.
    If present, read planting_date per field and emit a RunRequest for each
    date from planting_date up to today, targeting our ndvi_by_field asset.
    """
    s3 = context.resources.minio.get_s3_client()
    bucket = context.resources.minio.bucket_name

    # list inputs
    objs = s3.list_objects_v2(Bucket=bucket, Prefix="input_data/").get("Contents", [])
    keys = {o["Key"] for o in objs}
    if not {"input_data/bounding_box.geojson", "input_data/fields.geojson"}.issubset(keys):
        return SkipReason("waiting for both bounding_box.geojson and fields.geojson")

    # load fields
    fields_obj = s3.get_object(Bucket=bucket, Key="input_data/fields.geojson")
    gdf = gpd.read_file(StringIO(fields_obj["Body"].read().decode()))
    gdf["planting_date"] = pd.to_datetime(gdf["planting_date"])

    today = datetime.utcnow().date()
    requests = []
    for _, row in gdf.iterrows():
        fid = row["field_id"]
        start = row["planting_date"].date()
        end = today
        # for safety, cap at 5 years
        if end > start + timedelta(days=365 * 5):
            end = start + timedelta(days=365 * 5)
        for dt in pd.date_range(start=start, end=end):
            partition = dt.strftime("%Y-%m-%d")
            run_key = f"{fid}-{partition}"
            requests.append(
                RunRequest(
                    run_key=run_key,
                    tags={"field_id": str(fid)},
                    partition_key=partition,
                )
            )

    if not requests:
        return SkipReason("no new backfill partitions to launch")

    context.log.info(f"Enqueuing {len(requests)} NDVI runs")
    return requests
