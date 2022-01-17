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
from data_pipeline import util
from data_pipeline.zonal_stats import zonal_stats
import shutil
from data_pipeline.util import scantree
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
    :param src_vect_path: str, path to vect dataset
    :param stats_dict: dict representing  the results of computing func_name
                        inside the polygons of the first layer within the raster with id equal to first layer name
    :param stat_func: str, default=mean
    :return:
    """

    layer = src_ds.GetLayer(0)
    ldef = layer.GetLayerDefn()

    field_names = [ldef.GetFieldDefn(i).GetName() for i in range(ldef.GetFieldCount())]
    flist = [e for e in src_ds.GetFileList() if ".shp" in e]
    if flist:
        fn = f'to {flist[0]}'
    else:
        fn = ''

    if not field_name in field_names:
        logger.info(f'Adding field {field_name} {fn}')
        stat_field = ogr.FieldDefn(field_name, ogr.OFTReal)
        stat_field.SetWidth(15)
        stat_field.SetPrecision(2)
        layer.CreateField(stat_field)


    layer.StartTransaction()
    #TODO remove next line 100%
    sd = {int(k): v for (k, v) in stats_dict.items()}

    for feat in layer:
        fid = feat.GetFID()
        try:
            v = sd[fid][stat_func]
            feat.SetField(field_name, v )
            layer.SetFeature(feat)
        except Exception as e:
            logger.warning(f'No {field_name} value was assigned to feature {fid} {e} ')

    layer.CommitTransaction()
    layer.ResetReading()
    layer = None









def run(raster_layers_csv_blob=None, vector_layers_csv_blob=None,
        sas_url=None,
        store_attrs_per_vector=True,
        out_vector_path=None,
        vector_format={'ESRI Shapefile':'.shp'},
        dst_srs=3857
        ):

    assert out_vector_path not in ('', None), f'Invalid out_vector_path={out_vector_path}'

    assert  vector_format, f'Invalid vector_format={vector_format}. Valid options are {util.SUPPORTED_FORMATS} '

    vformat, ext = next(iter(vector_format.items()))

    assert vformat  in util.SUPPORTED_FORMATS, f'Invalid format={vector_format}. Valid options are {util.SUPPORTED_FORMATS}'

    per = 'vector' if store_attrs_per_vector else 'raster'

    logger.info(f'Going to store vector tiles per {per}')

    wmercP = osr.SpatialReference()
    wmercP.ImportFromEPSG(dst_srs)

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

                if out_vector_path not in ('', None):
                    vect_path = os.path.join(out_vector_path, 'in')
                else:
                    vect_path = f'/vsimem/{vfile_name}'

                #TODO do not forget to set vecpath ot vsimem

                vds, prj = util.fetch_az_shapefile(  blob_path=src_vector_blob_path,
                                                client_container=c,
                                                alternative_path='/data/sids/tmp/test/in')

                #print_field_names(vds,'aaa')
                vector_datasets[vid] = vds, prj

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
                # standardize
                #TODO remove alternative_path
                stdz_ds = standardize(src_blob_path=src_raster_blob_path,band=band, alternative_path='/data/sids/tmp/test')
                #iterate over vectors and
                for vds_id, t in vector_datasets.items():
                    vds, src_prj = t

                    srf = os.path.join('/data/sids/tmp/test', f'{rid}_st.json')

                    if not os.path.exists(srf) or os.path.getsize(srf) == 0:

                        stat_result = zonal_stats(raster_path_or_ds=stdz_ds, vector_path_or_ds=vds, band=band)
                        with open(srf, 'w') as out:
                            json.dump(stat_result, out)

                    else:
                        with open(srf) as infl:
                            stat_result = json.load(infl)


                    lname = rid


                    if store_attrs_per_vector:
                        logger.debug(f'Storing attrs per vector')
                        web_merc_vect_path = f'{os.path.join("/vsimem", vds_id)}{ext}'

                        try:
                            logger.info(f'Attempting to open {web_merc_vect_path}')
                            web_merc_ds =  gdal.OpenEx(web_merc_vect_path, gdal.OF_UPDATE|gdal.OF_VECTOR)

                        except Exception as e:
                            logger.info(f'Creating layer {lname} in {web_merc_vect_path}')

                            opts = gdal.VectorTranslateOptions(format=vformat,
                                                               dstSRS=wmercP.ExportToWkt(),
                                                               srcSRS=src_prj.ExportToWkt(),
                                                               reproject=True,
                                                               addFields=True,
                                                               # datasetCreationOptions=[],
                                                               # layerCreationOptions=['COORDINATE_PRECISION=5',
                                                               #                       'RFC7946=YES'],
                                                               layerName=lname,
                                                               skipFailures=True,
                                                               )
                            flist = [e for e in vds.GetFileList() if ".shp" in e]
                            if flist:
                                fn = flist[0]
                                logger.info(f'Reprojecting {fn} to {web_merc_vect_path}')
                            # reproject
                            web_merc_ds = gdal.VectorTranslate(destNameOrDestDS=web_merc_vect_path, srcDS=vds,
                                                               options=opts)

                        store_stats(src_ds=web_merc_ds, stats_dict=stat_result, field_name=rid)

                    else:
                        logger.debug('Storing attrs per raster')
                        web_merc_vect_path_json = f'{os.path.join(out_vector_path or "/vsimem", rid, vds_id)}.json'
                        web_merc_vect_path = f'{os.path.join("/vsimem", rid, vds_id)}{ext}'

                        basedir = os.path.dirname(web_merc_vect_path_json)
                        if not os.path.exists(basedir):
                            util.mkdir_recursive(basedir)
                        try:

                            src_ds =  gdal.OpenEx(web_merc_vect_path, gdal.OF_UPDATE|gdal.OF_VECTOR)
                            driver = src_ds.GetDriver()
                            driver.Delete(web_merc_vect_path)
                            src_ds = None
                            logger.info(f'Removed {web_merc_vect_path}')
                        except Exception as e:

                            pass

                        logger.info(f'Creating layer {lname} in {web_merc_vect_path}')

                        opts = gdal.VectorTranslateOptions(format=vformat,
                                                           dstSRS=wmercP.ExportToWkt(),
                                                           srcSRS=src_prj.ExportToWkt(),
                                                           reproject=True,
                                                           addFields=True,
                                                           # datasetCreationOptions=[],
                                                           # layerCreationOptions=['COORDINATE_PRECISION=5',
                                                           #                       'RFC7946=YES'],
                                                           layerName=lname,
                                                           skipFailures=True,
                                                           )
                        logger.info(f'Reprojecting {vds.GetFileList()} to {web_merc_vect_path}')
                        # reproject
                        web_merc_ds = gdal.VectorTranslate(destNameOrDestDS=web_merc_vect_path, srcDS=vds,
                                                           options=opts)




                        store_stats(src_ds=web_merc_ds, stats_dict=stat_result, field_name=rid )
                        web_merc_ds_json = gdal.VectorTranslate(destNameOrDestDS=web_merc_vect_path_json, srcDS=web_merc_ds,
                                                            format='GeoJSON'
                                                           )
                        web_merc_ds_json = None
                        web_merc_ds = None

                nrp+=1


                logger.info(f'Finished processing raster no:{nrp}')
                if nrp == 3:

                    break

        for vds_id, t in vector_datasets.items():
            vds, prj = t
            web_merc_vect_path = f'{os.path.join("/vsimem", vds_id)}{ext}'
            web_merc_vect_path_json = f'{os.path.join(out_vector_path, vds_id)}.json'
            basedir = os.path.dirname(web_merc_vect_path_json)
            if not os.path.exists(basedir):
                util.mkdir_recursive(basedir)
            web_merc_ds_json = gdal.VectorTranslate(destNameOrDestDS=web_merc_vect_path_json, srcDS=web_merc_vect_path,
                                                format='GeoJSON'
                                                )

            web_merc_ds_json = None


            if vds:
                fpl = vds.GetFileList()
                vds = None
                prj = None
                if fpl:
                    for fp in fpl:
                        if 'vsimem' in fp:
                            logger.info(f'Unlinking {fp}')
                            gdal.Unlink(fp)


def print_field_names(src_ds, aname):
    layer = src_ds.GetLayer(0)
    ldef = layer.GetLayerDefn()
    layer.ResetReading()
    field_names = [ldef.GetFieldDefn(i).GetName() for i in range(ldef.GetFieldCount())]
    fpath = src_ds.GetFileList()
    if not fpath:
        fpath = 'NA'
    else:
        fpath = fpath[0]
    logger.info(f'{src_ds} features {src_ds.GetDriver().LongName} format')
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
        out_vector_path='/data/sids/tmp/test/out/'
       )







