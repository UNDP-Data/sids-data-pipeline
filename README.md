# SIDS data processing pipeline

## Intro

**Small Islands Developing States (SIDS)** is a group of island states spatially disjoint located all over the world. This data pipeline can be used to pre-process and generate the bulk of spatial data for the SIDS platform [geospatial application](https://sids-dashboard.github.io/SIDSDataPlatform/main.html). The pipeline computes zonal stats for a number of vector layers from a number of raster layers and converts  the results into
MapBox vector tiles (.pbf) and stores them in an Azure Blob storage container. The specs for the raster and vector files are
fetched from CSV files stored in same Azure Blob storage. Both, the source and sink data is hosted inside and Azure Blob
storage container managed by UNDP GeoAnalytics.

## Structure

The data pipeline consists of three fucntional blocks

#### 1. Sources

The input consists of paths from where the raster and vector CSV spec files are downloaded. Obviously an Azure container SAS
url is required to access the CSV files. This can be provided either as a command line argument or an env. variable called
 `SAS_SIDS_CONTAINER` can be created for the same purpose.

### 2. Processing

#### 1. Standardization

Ensures all data is brought to a common set of specs before it is processed:

- ESPG:4326 projection
- clipped to lonmin=-180, lonmax=180, latmin=-35, latmax=35

#### 2. Zonal stats

Zonal statistics (mode and mean) are computed for each feature in a given vector from every available raster.

#### 3. Export to MVT using tippecanoe

The results from zonal stats are added to the vector geometries in each  layer in attribute columns. Depending on the provided
arguments the attributes are either added to the same vector layer or new layers are created for every raster layer.
Next the MVT are exported using tippecanoe.

### 3. Upload to Azure blob

In the last step the whole folder that contains the MVT files is uploded asynchronously to the Azure Blob container.
Optionally the MVT folder can be also removed for the local file system.

## Notes

Originally the plan was to use GDAL to do the whole job but this was a no go because GDAL could not translate  some of the
vector layers to MVT format. Apparently the admin layers contain features whose bounding box spans accross the whole world.
The GDAL MVT driver can not handle these kind of of geometries and was getting struck in infinite loops. An [issue](https://github.com/OSGeo/gdal/issues/5109)
was submitted and if this will be resolved we could switch to GDAL entirely to do the job.

An alternative solution is to do some pre-processing on the original vector data as to ensure the GDAL will be able to process it.

As a result we resorted to using tippecanoe to export the data to MVT. For practical purposes, we used the dockerized version of tippecanoe because
it is relatively straightforward to set up.

## Hands on

The pipeline is deployed as a Docker image and this means [Docker](https://www.docker.com/) needs to be installed and available.

```bash
#start by creating a folder
mkdir sidspipeline
cd sidspipeline
```

### 1. Create an .env file holding the Azure credentials

The SIDS data is stored in an Azure blob container so a SAS  using your favourite editor create a .env text file and add
`SAS_SIDS_CONTAINER=url` where url is a full Azure SAS URL.

### 2. Download the docker image

```bash
docker pull ghcr.io/undp-data/sids-data-pipeline:latest
```

### 3. run the pipeline

The interaction with the pipeline is done via a python script deployed as **sidspipeline**  command inside the container:

```bash
docker run  -ti --rm --name=sidsdatapipeline --env-file .env -v /data:/data ghcr.io/undp-data/sids-data-pipeline:latest sidspipeline
```

Here we are instructing docker to deploy a container using the previously pulled image and xecute the *sidspipeline* command inside the container:

- is named sidsdatapipeline - this is useful to be able to identify the container in case it needs to be stopped
- is using/passed the env variables defined in the .env file through `--env-file` arg
- is mounting local `/data` folder as `/data` inside the container
- is immediately removed `--rm` after the  data pipeline script finishes

The result of the above command is :

```bash
docker run  -ti --rm --name=sidsdatapipeline --env-file .env -v /data:/data ghcr.io/undp-data/sids-data-pipeline:latest sidspipeline

usage: sidspipeline [-h] -rb RASTER_LAYERS_CSV_BLOB [-vb VECTOR_LAYERS_CSV_BLOB] [-su SAS_URL] [-ov OUT_VECTOR_PATH] [-ub UPLOAD_BLOB_PATH] [-ag AGGREGATE_VECT]
                    [-rm REMOVE_TILES_AFTER_UPLOAD] [-cf CACHE FOLDER] [-sm SAMPLE_MODE] [-d DEBUG]

Run the SIDS data pipeline. The pipeline computes zonal stats for a number of vector layers from a number of raster layers. The results are converted into MapBox Vector Tile format
and uploaded to an Azure Blob storage container. The specs for the raster and vector files are fetched from CSV files stored in same Azure Blob storage.

optional arguments:
  -h, --help            show this help message and exit
  -rb RASTER_LAYERS_CSV_BLOB, --raster_layers_csv_blob RASTER_LAYERS_CSV_BLOB
                        relative path(in respect to the container) of the CSV file that holds info in respect to vector layers (default: None)
  -vb VECTOR_LAYERS_CSV_BLOB, --vector_layers_csv_blob VECTOR_LAYERS_CSV_BLOB
                        relative path (in respect to the container) if the CSV file that contains info related to the raster files to be processed (default: None)
  -su SAS_URL, --sas_url SAS_URL
                        MS Azure SAS url granting rw access to the container. Alternatively the environmentvariable SAS_SIDS_CONTAINER can be used to supply a SAS URL (default: None)
  -ov OUT_VECTOR_PATH, --out_vector_path OUT_VECTOR_PATH
                        abs path to a folder where output data (MVT and JSON) is going to be stored (default: None)
  -ub UPLOAD_BLOB_PATH, --upload_blob_path UPLOAD_BLOB_PATH
                        relative path (to the container) where the MVT data will be copied (default: None)
  -ag AGGREGATE_VECT, --aggregate_vect AGGREGATE_VECT
                        determines if the zonal statistics will be accumulated into the vector layers as columns in the attr table. If False, a new vector layer/vector tile will be
                        created for every combination of raster and vector layers (default: True)
  -rm REMOVE_TILES_AFTER_UPLOAD, --remove_tiles_after_upload REMOVE_TILES_AFTER_UPLOAD
                        if the tiles should be removed after upload (default: True)
  -cf CACHE_FOLDER, --cache_folder CACHE_FOLDER
                        Abs path to a folder where input data can be cached and reread an next launch (default: None)
  -sm SAMPLE_MODE, --sample_mode SAMPLE_MODE
                        if True the pipeline will stop after collecting one raster and vector file (default: False)
  -d DEBUG, --debug DEBUG
                        debug mode on/off (default: False)

```

The arguments are more or less self explanatory and match  the structure of the pipeline as described in the [Structure](#Structure) section

```bash
# run the script on


    docker run  -ti --rm \
    --name=sidsdatapipeline \
    --env-file .env \
    -v /data:/data \
    ghcr.io/undp-data/sids-data-pipeline:latest sidspipeline \

        -rb=config/attribute_list_updated.csv \
        -vb=config/vector_list.csv \
        -ov /data/sids/tmp/test1 \
        -ub=vtiles1 \
        -ag=True \
        -cf=/data/sids/tmp/test


    # or a one liner
    docker run  -ti --rm --name=sidsdatapipeline --env-file .env -v /data:/data ghcr.io/undp-data/sids-data-pipeline:latest sidspipeline -rb=config/attribute_list_updated.csv -vb=config/vector_list.csv -ov /data/sids/tmp/test1 -ub=vtiles1 -ag=True -ap=/data/sids/tmp/test


    2022-01-22 16:18:18-pipeline.py:run:157:INFO:Going to compute and store vector tiles per vector
    2022-01-22 16:18:18-util.py:fetch_vector_from_azure:124:INFO:Reading rawdata/Shapefiles/admin/admin0-sids-4326.shp from /data/sids/tmp/test/admin0-sids-4326.shp
    2022-01-22 16:18:19-pipeline.py:run:258:INFO:/vsiaz/sids/rawdata/Raw GIS Data/Atlas/Data/Raster/1a1_band.tif is going to be aggregated for zonal stats
    2022-01-22 16:18:19-pipeline.py:run:280:INFO:Going to process 1 raster file/s and 1 vector file/s
    2022-01-22 16:18:19-standardization.py:standardize:22:INFO:Standardizing /vsiaz/sids/rawdata/Raw GIS Data/Atlas/Data/Raster/1a1_band.tif
    2022-01-22 16:18:19-standardization.py:standardize:37:INFO:Reusing /data/sids/tmp/test/1a1_band_stdz.tif instead of /vsiaz/sids/rawdata/Raw GIS Data/Atlas/Data/Raster/1a1_band.tif
    2022-01-22 16:18:19-pipeline.py:run:293:INFO:Processing zonal stats for raster 1a1 and admin0
    2022-01-22 16:18:19-pipeline.py:run:306:INFO:Reusing zonal stats from /data/sids/tmp/test/admin0_1a1_stats.json
    2022-01-22 16:18:19-pipeline.py:add_field_to_vector:59:INFO:Adding field 1a1 to /data/sids/tmp/test/admin0-sids-4326.shp
    2022-01-22 16:18:22-pipeline.py:run:373:INFO:Exporting accumulated admin0 to MVT
    2022-01-22 16:18:22-pipeline.py:run:393:INFO:Exporting /data/sids/tmp/test/admin0-sids-4326.shp to  /data/sids/tmp/test1/json/admin0.geojson
    2022-01-22 16:18:35-util.py:export_with_tippecanoe:248:INFO:Exporting /data/sids/tmp/test1/json/admin0.geojson to /data/sids/tmp/test1/tiles/admin0
    2022-01-22 16:18:47-pipeline.py:run:428:INFO:Going to upload vector tiles from /data/sids/tmp/test1/tiles to container sids/vtiles1
    Uploading ... :  11%

```

The above command resulted in executing the SIDS data pipeline with following setup:

- "-rb=config/attribute_list_updated.csv" - read the raster config specs from a CSV file located in the Azure blob path **config/attribute_list_updated.csv**
- "-vb=config/vector_list.csv" - read the vector config specs from a CSV file located in the Azure blob path **config/vector_list.csv**
- "-ov /data/sids/tmp/test1" - store the output vector tiles in the */data/sids/tmp/test1* folder. Note that a "*tiles*" folder will be created in this path
and the tiles will be stored there
- "-ub=vtiles1" uplod the output tiles into the **vtiles** fodler on the remote Azure storage specified by `SAS_SIDS_CONTAINER` env var
- "-ag=True" - aggregate the vector tiles per vector, that ie, the zontal stats for all raster files will be stored as attributes in each  input vector dataset
The result of this is that the number of generated vector tiles datasets is equal to the number of vector layers. In this arg is set to **False** then a vector tiles dataset
would be generated for each input raster
  - Here is the output for agg=True value

  ```bash
    janf@hyda:/work/py/depl$ tree -d -L 3 /data/sids/tmp/test1
    /data/sids/tmp/test1
    └── tiles
        └── admin0
            ├── 0
            ├── 1
            ├── 10
            ├── 11
            ├── 12
            ├── 2
            ├── 3
            ├── 4
            ├── 5
            ├── 6
            ├── 7
            ├── 8
            └── 9

    ```

  - and here is  the output for the ag=False

  ```bash
    janf@hyda:/work/py/depl$ tree -d -L 4 /data/sids/tmp/test1
    /data/sids/tmp/test1
    └── tiles
        └── 1a1
            └── admin0
                ├── 0
                ├── 1
                ├── 10
                ├── 11
                ├── 12
                ├── 2
                ├── 3
                ├── 4
                ├── 5
                ├── 6
                ├── 7
                ├── 8
                └── 9

  ```

  - "-ap=/data/sids/tmp/test" - store the intermediary (downloaded from Azure) geo-spatial data in this folder and reuse it on next iterations. This should be used for
  testing and development only or is working with the pipeline on a machine with slow internet connection

## Development

The pipeline can be modified by means of pull requests. Every tagged pull request will generate a new image.
