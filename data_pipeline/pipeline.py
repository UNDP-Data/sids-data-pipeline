import logging
import os
from data_pipeline.standardization import standardize
from data_pipeline.azblob import HandyContainerClient
import io
import csv
import asyncio
from osgeo import gdal, ogr, osr

import json
from dotenv import load_dotenv
from azure.storage.blob import ContainerClient
from data_pipeline.zonal_stats import fetch_az_shapefile, fetch_az_shapefile_direct
from data_pipeline.zonal_stats import zonal_stats

gdal.UseExceptions()

load_dotenv()
logger = logging.getLogger(__name__)

def get_csv_cfgs(sas_url=None, cfg_blob_path=None, sync=False):

    if not sync:
        async def fetch(sas_url, cfg_blob_path):
            async with HandyContainerClient(sas_url=sas_url) as cc:
                return [item['name'] async for item in cc.list_blobs_async(name=cfg_blob_path)]


        yield from asyncio.run(fetch(sas_url=sas_url, cfg_blob_path=cfg_blob_path))

    else:
        with HandyContainerClient(sas_url=sas_url) as cc:
            for l in cc.listblobs(name=cfg_blob_path):
                print(l)


def store_stats(src_ds=None, stats_dict = None, stat_func='mean', field_name=None):
    """
    Store the zonal stat results located in stat_dict  in the first layer of the src_ds.
    :param src_ds: ogr.DataSource, instance
    :param stats_dict: dict representing  the results of computing func_name
                        inside the polygons of the first layer within the raster with id equal to first layer name
    :param stat_func: str, default=mean
    :return:
    """
    layer = src_ds.GetLayer(0)
    print(src_ds.GetFileList())
    drv = src_ds.GetDriver()
    print(drv.LongName, layer)
    ldef = layer.GetLayerDefn()

    field_names = [ldef.GetFieldDefn(i).GetName() for i in range(ldef.GetFieldCount())]
    if not field_name in field_names:
        stat_field = ogr.FieldDefn(field_name, ogr.OFTReal)
        stat_field.SetWidth(15)
        stat_field.SetPrecision(2)
        layer.CreateField(stat_field)


    layer.StartTransaction()
    #TODO remove next line 100%
    sd = {int(k):v for (k,v) in stats_dict.items()}

    for feat in layer:
        fid = feat.GetFID()
        try:
            v = sd[fid][stat_func]
            feat.SetField(field_name, v )
            layer.SetFeature(feat)
        except Exception:
            logger.warning(f'No {field_name} value was assigned to feature {fid} ')

    layer.CommitTransaction()








def run(raster_layers_csv_blob=None, vector_layers_csv_blob=None,
        sas_url=None,
        store_attrs_per_vector=False,
        alternative_path=None,
        vector_format='MVT'):
    supported_formats = 'ESRI Shapefile', 'MVT'
    assert vector_format in supported_formats, f'Invalid format={format()}. Valid options are {",".join(supported_formats)}'
    ext = '.shp' if 'ESRI' in vector_format else '.pbf'

    wmercP = osr.SpatialReference()
    wmercP.ImportFromEPSG(3857)

    # make a sync container client
    with ContainerClient.from_container_url(container_url=sas_url) as c:
        vector_datasets = dict()
        vector_csv_stream = c.download_blob(vector_layers_csv_blob)

        with io.BytesIO() as vstr:
            vector_csv_stream.readinto(vstr)
            vstr.seek(0)
            vlines = (line.decode('utf-8') for line in vstr.readlines())
            vreader = csv.DictReader(vlines)

            for csv_vector_row in vreader:


                vid = csv_vector_row['vector_id']
                if '\\' in csv_vector_row['file_name']:
                    vfile_name = csv_vector_row['file_name'].replace('\\', '/')
                else:
                    vfile_name = csv_vector_row['file_name']
                if '\\' in csv_vector_row['path']:
                    vpath = csv_vector_row['path'].replace('\\', '/')
                else:
                    vpath = csv_vector_row['path']

                # src_vector_blob_path = os.path.join('/vsiaz/sids/rawdata', vpath, vfile_name)
                src_vector_blob_path = os.path.join('rawdata', vpath.replace('Shapefile', 'Shapefiles'), vfile_name)
                vds = fetch_az_shapefile_direct(blob_path=src_vector_blob_path, client_container=c, alternative_path=None)
                vector_datasets[vid] = vds
                break

        nrp = 0
        # fetch a stream
        rast_csv_stream = c.download_blob(raster_layers_csv_blob)
        # push the binary stream into RAM
        with io.BytesIO() as rstr:
            rast_csv_stream.readinto(rstr)
            # need to position at the beginning
            rstr.seek(0)
            # cretae a generator of text lines from the binary lines
            rlines = (line.decode('utf-8') for line in rstr.readlines())
            # instantitae  a reader
            rreader = csv.DictReader(rlines)
            for raster_csv_row in rreader:

                rid = raster_csv_row['attribute_id']
                band = int(raster_csv_row['band'])
                if '\\' in raster_csv_row['file_name']:
                    rfile_name = raster_csv_row['file_name'].replace('\\', '/')
                else:
                    rfile_name = raster_csv_row['file_name']

                if '\\' in raster_csv_row['path']:
                    rpath = raster_csv_row['path'].replace('\\', '/')
                else:
                    rpath = raster_csv_row['path']

                src_raster_blob_path = os.path.join('/vsiaz/sids/', rpath, rfile_name)
                logger.debug(f'Processing {src_raster_blob_path}')
                # stanndardize
                stdz_ds = standardize(src_blob_path=src_raster_blob_path,band=band, alternative_path=alternative_path)
                #iterate over vectors and
                for vds_id, vds in vector_datasets.items():
                    #print(vds_id, vds, vds.GetFileList())
                    srf = os.path.join(alternative_path, f'{rid}_st.json')

                    if not os.path.exists(srf) or os.path.getsize(srf) == 0:

                        stat_result = zonal_stats(raster_path_or_ds=stdz_ds, vector_path_or_ds=vds, band=band)
                        with open(srf, 'w') as out:
                            json.dump({int(k):v for (k, v) in stat_result.items()}, out)

                    else:
                        with open(srf) as infl:
                            stat_result = json.load(infl)


                    if store_attrs_per_vector:
                        web_merc_vect_path = f'/vsimem/{vds_id}{ext}'
                        web_merc_vect_path = f'{os.path.join(alternative_path,vds_id)}{ext}'
                        web_merc_vect_path = f'/vsimem/{vds_id}{ext}'
                        lname = vds_id
                        access_mode = 'append'

                    else:
                        web_merc_vect_path = f'/vsimem/{rid}{ext}'
                        web_merc_vect_path = f'{os.path.join(alternative_path,rid)}{ext}'
                        web_merc_vect_path = f'/vsimem/{rid}{ext}'
                        lname = rid
                        access_mode = None
                    try:
                        web_merc_ds = gdal.OpenEx(web_merc_vect_path, gdal.OF_UPDATE)


                    except Exception as e:
                        logger.info(f'Creating layer {lname} in {web_merc_vect_path}')
                        opts = gdal.VectorTranslateOptions(format=vector_format,
                                                           accessMode='overwrite',
                                                           dstSRS=wmercP,
                                                           reproject=True,
                                                           addFields=True,
                                                           # datasetCreationOptions=[],
                                                           # layerCreationOptions=['SPATIAL_INDEX=YES'],
                                                           #layerName=lname
                                                           )

                        web_merc_ds = gdal.VectorTranslate(destNameOrDestDS=web_merc_vect_path, srcDS=vds, options=opts)

                    print(type(vds), vds.GetSpatialRef().ExportToProj4())
                    print_field_names(web_merc_ds, 'test')
                    # if not os.path.exists(web_merc_vect_path):
                    #     logger.info(f'Creating layer {lname} in {web_merc_vect_path}')
                    #     opts = gdal.VectorTranslateOptions(format=vector_format,
                    #                                        accessMode='overwrite',
                    #                                        dstSRS=wmercP,
                    #                                        reproject=True,
                    #                                        addFields=True,
                    #                                        #datasetCreationOptions=[],
                    #                                        #layerCreationOptions=['SPATIAL_INDEX=YES'],
                    #                                        layerName=lname
                    #                                        )
                    #
                    #     web_merc_ds = gdal.VectorTranslate(destNameOrDestDS=web_merc_vect_path, srcDS=vds,options=opts)
                    # else:
                    #     web_merc_ds = gdal.OpenEx(web_merc_vect_path, gdal.OF_UPDATE)

                    #print(web_merc_ds.GetLayerCount(), web_merc_ds.GetLayer(0).GetName(), web_merc_ds.GetLayer(0).GetFeatureCount())
                    #print(web_merc_ds.GetFileList())
                    # if not store_attrs_per_vector:
                    #     mvt_opts = gdal.VectorTranslateOptions( for
                    #
                    #     )
                    #     web_merc_mvt = gdal.VectorTranslate(destNameOrDestDS=web_merc_vect_path, srcDS=vds,options=opts)


                    store_stats(src_ds=web_merc_ds, stats_dict=stat_result, field_name=rid )
                    print_field_names(web_merc_ds, lname)
                    nrp+=1



                    #web_merc_ds = None
                if nrp == 3:

                    break

        for vds_id, vds in vector_datasets.items():
            fpl = vds.GetFileList()
            vds = None
            if fpl:
                for fp in fpl:
                    logger.info(f'Unlinking {fp}')
                    gdal.Unlink(fp)


def print_field_names(src_ds, aname):
    layer = src_ds.GetLayer(0)
    ldef = layer.GetLayerDefn()

    field_names = [ldef.GetFieldDefn(i).GetName() for i in range(ldef.GetFieldCount())]
    fpath = src_ds.GetFileList()
    if not fpath:
        fpath = 'NA'
    else:
        fpath = fpath[0]

    logger.info(
        f'Layer {layer.GetName()} from {fpath} has {layer.GetFeatureCount()}'
        f' features and {",".join(field_names)} in {aname}'
    )




if __name__ == '__main__':

    logging.basicConfig()
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                                             "%Y-%m-%d %H:%M:%S"))

    rlogger = logging.getLogger()
    azlogger = logging.getLogger('azure.storage')
    azlogger.setLevel(logging.NOTSET)



    # remove the default stream handler and add the new on too it.
    rlogger.handlers.clear()
    rlogger.addHandler(sthandler)

    rlogger.setLevel('INFO')
    rlogger.name = os.path.split(__file__)[-1]



    csv_config_blob_path = 'config'
    sas_url = 'https://undpngddlsgeohubdev01.blob.core.windows.net/sids?sp=racwdl&st=2022-01-06T21:09:27Z&se=2032-01-07T05:09:27Z&spr=https&sv=2020-08-04&sr=c&sig=XtcP1UUnboo7gSVHOXeTbUt0g%2FSV2pxG7JVgmZ8siwo%3D'

    #0 FETCH configs

    # configs = get_csv_cfgs(sas_url=sas_url,cfg_blob_path=csv_config_blob_path)
    # for cfg in configs:
    #     print(cfg)

    # RUN



    run(
        raster_layers_csv_blob='config/attribute_list_updated.csv',
        vector_layers_csv_blob='config/vector_list.csv',
        sas_url=sas_url,
        alternative_path='/data/sids/tmp'
       )







