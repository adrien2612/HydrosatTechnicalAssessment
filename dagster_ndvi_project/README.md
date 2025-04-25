# Dagster NDVI Processing Pipeline

This pipeline processes NDVI (Normalized Difference Vegetation Index) data for agricultural fields, using Sentinel-2 satellite imagery.

## Key Features

- **Field-Specific Processing**: Each field is processed individually based on its unique planting date
- **Automatic Backfill**: Upon detecting a fields.geojson file upload, the system automatically schedules processing runs for each field from its planting date to the current date
- **Per-Field Output Structure**: Results are organized by field ID and date in the MinIO bucket
- **Visualization**: Each field gets its own NDVI visualization for each date

## Input Requirements

Upload these files to the MinIO bucket in the `input_data` folder:

1. **fields.geojson** - Contains field boundaries with required properties:
   - `field_id`: Unique identifier for each field
   - `planting_date`: Date when the crop was planted (YYYY-MM-DD)
   - `crop_type`: Type of crop planted

2. **bounding_box.geojson** - Overall bounding box containing all fields

Example fields.geojson:
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "field_id": "F001",
        "planting_date": "2023-03-15",
        "crop_type": "corn"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [11.02, 48.02],
            [11.04, 48.02],
            [11.04, 48.04],
            [11.02, 48.04],
            [11.02, 48.02]
          ]
        ]
      }
    }
  ]
}
```

## Output Structure

The pipeline outputs results to the MinIO bucket in the following structure:

```
output_data/
  ├── {field_id}/
  │   ├── {date}/
  │   │   ├── ndvi_results.json  # Results for this field on this date
  │   │   └── ndvi_plot.png      # NDVI visualization for this field
  │   └── ...
  ├── ...
  └── {date}/                    # Combined results for all fields by date
      └── ndvi_results.json      # All field results for this date
```

## How Backfill Works

1. When you upload a fields.geojson file, the system will:
   - Parse the file to extract field IDs and planting dates
   - For each field, create processing jobs from its planting date to the current date
   - Store results in field-specific directories

2. The pipeline only processes fields whose planting date is on or before the processing date.

3. Each field's NDVI data is calculated and stored independently, allowing for efficient parallel processing.

## Monitoring

You can monitor processing jobs and view results in the Dagster UI at: http://localhost:8080

## Accessing Results

Results are available in the MinIO bucket at:
- Web interface: http://localhost:9001 (login with credentials from the setup)
- API endpoint: http://localhost:9000 