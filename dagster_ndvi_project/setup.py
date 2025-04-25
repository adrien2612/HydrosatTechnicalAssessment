from setuptools import find_packages, setup

setup(
    name="dagster_ndvi_project",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-aws",
        "dagster-k8s",
        "pystac-client",
        "rasterio",
        "geopandas",
        "shapely",
        "numpy",
        "pandas",
        "xarray",
        "rioxarray",
        "boto3",
    ],
) 