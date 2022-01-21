# SIDS data processing pipeline

## Intro
**Small Islands Developing States (SIDS)** is a group of island states spatially disjoint located all over the world.  
This data pipeline can be used to pre-process and generate the bulk of spatial data for the SIDS platform [geospatial application](https://sids-dashboard.github.io/SIDSDataPlatform/main.html)  
The pipeline computes zonal stats for a number of vector layers from a number of raster layers and converts  
the results into MapBox vector tiles (.pbf) and stores them in an Azure Blob storage container.  
The specs for the raster and vector files are fetched from CSV files stored in same Azure Blob storage.  
Both, the source and sink data is hosted inside and Azure Blob storage container managed by UNDP GeoAnalytics.

## Structure

The data pipeline consists of three blocks

### 1. Sources

The input consists of paths from where the raster and vector CSV spec files are downloaded.
Obviously an Azure container SAS url is required to access the CSV files. This can be provided
either as a command line argument or an env. variable SAS_SIDS_CONTAINER can be created to store the url.

### 2. Processing


####        1. Standardization

Ensures all data is brought to a common set of specs:
    a) ESPG:4326 projection
    b) clipped to lonmin=-180, lonmax=180, latmin=-35, latmax=35

####        2. Zonal stats

Zonal statistics (mode and mean) are computed for each feature in a  
given vector from every available raster

####        3. Export to MVT using tippecanoe

The results from zonal stats are added to the vector geometries in each  
layer in attribute columns. Depending on the provided arguments the  
attributes are either added to the same vector layer or new layers are  
created for every raster layer. Then the MVT are exported using tippecanoe.


### 3. Upload to Azure blob
 In the last step the whole folder that contains the MVT files is uploded  
 asynchronously to an Azure Blob container. Optionally the fodler can be also removed



## Notes

Originally the plan was to use GDAL to do the whole job but this was a no go because GDAL  
could not translate  some of the vector layers to MVT format. Apparently the admin layers contain features  
whose bounding box spans accross the whole world. The GDAL MVT driver can not handle these kind of  
of geometries and was getting struck in infinite loops. An [issue](https://github.com/OSGeo/gdal/issues/5109)
was submitted and if this will be resolved  
we could switch to GDAL entirely to do the job.

An alternative solution is to do som pre-processing on the original

As a result we resorted to using tippecanoe to export the data to MVT. For practical purposes,  
we used the dockerized version of tippecanoe as it is relatively straightforward to set up.




## Hands on
docker needs to be installed

### From GitHub

```bash
    # download the docker image
    
    docker pull ghcr.io/undp-data/sids-data-pipeline:latest
    
    # run the script
    
    
    docker run  -ti --rm --name=sidspipe --env-file .env -v /data:/data sids-data-pipeline:latest -rb=config/attribute_list_updated.csv -vb=config/vector_list.csv -ov /data/sids/tmp/test1 -ub=vtiles1 -ag=True -ap=/data/sids/tmp/test

    
```



