# SIDS data processing pipeline

## Intro

**Small Islands Developing States (SIDS)** is a group of island states spatially disjoint located all over the world. This data pipeline can be used to pre-process and generate the bulk of spatial data for the SIDS platform [geospatial application](https://data.undp.org/sids/geospatial-data). The pipeline computes zonal stats for a number of vector layers from a number of raster layers and converts the results into MapBox vector tiles (.pbf) and stores them in an Azure Blob storage container.

## Project Structure

Inputs are hosted on an Azure Container Blob, in the `inputs` folder of the `sids` container. Rasters and vectors are stored in the respective subfolders, as GeoPackages and GeoTiffs. The `batch.csv` file provides metadata about rasters. 

```shell
inputs
├── batch.csv
├── rasters
│   ├── data1.tif
│   ├── data2.tif
│   └── data3.tif
└── vectors
    ├── zone1.gpkg
    ├── zone2.gpkg
    └── zone3.gpkg
```

## Batch

Batch is the first sub-module, helping to import rasters from all throughout Azure blob storage into a single folder. This module takes a few hours to runn for . Reading the `batch.csv`, the following data standardizations take place:

- ZSTD compression
- ESPG:4326 projection
- clipped to lonmin=-180, lonmax=180, latmin=-35, latmax=35

## Pipeline

Pipeline is the second sub-module, taking the majority of time to run to generate zonal statistics and vector tiles. The pipeline is optimized to check if a vector/raster combination already exists at the destination, in which case it will be skipped.

## Setup

To get started, populate the .env file with values using the template, and log into Azure and Docker.

```shell
az login
docker login undpgeohub.azurecr.io
```

To run either the batch or pipeline, change directory into one of the following and run `./deploy.sh` from that subfolder.
