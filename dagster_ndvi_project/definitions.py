# definitions.py

from dagster import Definitions, EnvVar, FilesystemIOManager, define_asset_job, AssetKey
from dagster_aws.s3 import S3Resource
from dagster_k8s import k8s_job_executor

# Use relative imports within the package
from dagster_ndvi_project.dagster_ndvi_project.resources import MinioResource
from dagster_ndvi_project.dagster_ndvi_project.assets import compute_ndvi_raw
from dagster_ndvi_project.dagster_ndvi_project.sensors.optimized_ndvi_sensor import optimized_ndvi_sensor

# Local filesystem IO (for intermediate testing)
local_io_manager = FilesystemIOManager(base_dir="./data")

# K8s executor config — k3d is Kubernetes under the hood, so use the same executor.
k8s_executor = k8s_job_executor.configured({
    "job_namespace": "dagster",
    "image_pull_policy": "IfNotPresent",
    "service_account_name": "default",
    "max_concurrent": 3,
    "step_k8s_config": {
        "container_config": {
            "resources": {
                "requests": {"cpu": "250m", "memory": "512Mi"},
                "limits":   {"cpu": "1",   "memory": "1Gi"},
            }
        }
    },
})

ndvi_processing_job = define_asset_job(
    name="ndvi_processing_job",
    selection=[AssetKey("compute_ndvi_raw")],
    executor_def=k8s_executor,
)

definitions = Definitions(
    assets=[compute_ndvi_raw],
    sensors=[optimized_ndvi_sensor],
    jobs=[ndvi_processing_job],
    resources={
        "minio": MinioResource(
            endpoint_url=EnvVar("MINIO_ENDPOINT_URL"),
            aws_access_key_id=EnvVar("MINIO_ACCESS_KEY"),
            aws_secret_access_key=EnvVar("MINIO_SECRET_KEY"),
            bucket_name=EnvVar("MINIO_BUCKET_NAME"),
        ),
        "s3": S3Resource(
            endpoint_url=EnvVar("MINIO_ENDPOINT_URL"),
            aws_access_key_id=EnvVar("MINIO_ACCESS_KEY"),
            aws_secret_access_key=EnvVar("MINIO_SECRET_KEY"),
        ),
        "io_manager": local_io_manager,
    },
)
