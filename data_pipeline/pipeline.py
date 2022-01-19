import logging
import os
from data_pipeline.standardization import standardize
from data_pipeline.azblob import HandyContainerClient
import io
import csv
import asyncio
from osgeo import gdal, ogr
import json
from azure.storage.blob import ContainerClient
from data_pipeline import util
from data_pipeline.zonal_stats import zonal_stats
from azure.core.exceptions import  ResourceNotFoundError
import shutil


gdal.UseExceptions()


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


def add_field_to_vector(src_ds=None, stats_dict = None, stat_func='mean', field_name=None):
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



def remove_filed_from_vector(src_ds=None, field_name=None):
    """
    Remove filed_name from src_ds
    :param src_ds: instance of gdal.Dataset or ogr.DastaSource
    :param field_name: str, name fo the field
    :return:
    """


    layer = src_ds.GetLayer(0)
    ldef = layer.GetLayerDefn()

    field_names = [ldef.GetFieldDefn(i).GetName() for i in range(ldef.GetFieldCount())]
    flist = [e for e in src_ds.GetFileList() if ".shp" in e]
    if flist:
        fn = f'{flist[0]}'
    else:
        fn = ''

    if field_name in field_names:
        logger.info(f'Removing field {field_name} from {fn}')
        field_id = [ i for i in range(ldef.GetFieldCount()) if ldef.GetFieldDefn(i).GetName() == field_name].pop(0)
        layer.DeleteField(field_id)

    layer = None


def run(
            raster_layers_csv_blob=None,
            vector_layers_csv_blob=None,
            sas_url=None,
            store_attrs_per_vector=False,
            out_vector_path=None,
            upload_blob_path=None,
            remove_tiles_after_upload=False,
            # the args below should normally not be changed
            root_mvt_folder_name='tiles',
            root_geojson_folder_name = 'json',
            alternative_path=None

    ):
    """

    :param raster_layers_csv_blob: str, relative path(in respect to the container) of the CSV
            file that holds info in respect to vector layers
    :param vector_layers_csv_blob:str, relative path (in respect to the container) if the CSV
            file that contains info related to the raster files to be processed
    :param sas_url: str, MS Azure SAS surl granting rw access to the container
    :param store_attrs_per_vector: bool, default=??? determines if the zonal statistics
            will be accumulated into the vector layers as columns in the attr table. If False,
            a new vector layer/vector tile will be created for every combination of raster and vector
            layers

    :param out_vector_path:str, abs path to a folder where output data (MVT) is going to be stored
    :param upload_blob_path: str, relative path (to the container) where the MVT data will be copied

    :param remove_tiles_after_upload: bool, default=False, if True the MVT data will be removed
    :param root_mvt_folder_name: str, the name of the folder where all MVT's will be stored inside the
            out_vector_path folder
    :param root_geojson_folder_name: str, the name of the folder where the GeoJSON datat will be written
            inside the out_vector_path folder
    :param alternative_path: str, full path to a folder where the incoming thata that is downlaoded will be stored and
            cached. Used during developemnt
    :return:
    """
    assert out_vector_path not in ('', None), f'Invalid out_vector_path={out_vector_path}'


    per = 'vector' if store_attrs_per_vector else 'raster'

    logger.info(f'Going to store vector tiles per {per}')

    vsi_vect_paths = dict()
    vsiaz_rast_paths = dict()
    mvt_folder_paths =dict()

    # used to stop parsing all data during deve;
    rast_break_at = 3
    vect_break_at = 3

    missing_az_vectors = list()
    missing_az_rasters = list()
    # make a sync container client
    with ContainerClient.from_container_url(container_url=sas_url) as c:

        vector_csv_stream = c.download_blob(vector_layers_csv_blob)

        with io.BytesIO() as vstr:
            vector_csv_stream.readinto(vstr)
            vstr.seek(0)
            vlines = (line.decode('utf-8') for line in vstr.readlines())
            vreader = csv.DictReader(vlines)
            nv = 0
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


                # check if it exists
                try:
                    __ = c.list_blobs(name_starts_with=src_vector_blob_path)
                    _ = [e.name for e in __]
                    if not _:
                        raise ResourceNotFoundError(f'{src_vector_blob_path}')
                    del __
                    vsi_vect_path = util.fetch_vector_from_azure(
                        blob_path=src_vector_blob_path,
                        client_container=c,
                        alternative_path='/data/sids/tmp/test/in'
                    )
                except ResourceNotFoundError:
                    missing_az_vectors.append(src_vector_blob_path)
                    continue

                vsi_vect_paths[vid] = vsi_vect_path
                nv+=1
                if nv == vect_break_at:
                    break

        # fetch  raster stream
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
            nr = 0
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


                #TODO check if it exists
                try:
                    r = gdal.Info(src_raster_blob_path)
                    vsiaz_rast_paths[rid] = src_raster_blob_path, band
                    logger.info(f'{src_raster_blob_path} is going to be aggregated for zonal stats')
                except Exception as e:
                    missing_az_rasters.append(src_raster_blob_path)
                    continue
                nr+=1
                if rast_break_at is not None:
                    if nr == rast_break_at:
                        break



    no_proc_rast = 0
    n_rast_to_process = len(vsiaz_rast_paths)
    logger.info(f'Going to process {n_rast_to_process} raster file/s and {len(vsi_vect_paths)} vector file/s')

    for rds_id, tp in vsiaz_rast_paths.items():
        vsiaz_rds_path, band = tp
        # 1 STANDARDIZE
        stdz_rds = standardize(src_blob_path=vsiaz_rds_path, band=band, alternative_path='/data/sids/tmp/test')

        for vds_id, vds_path in vsi_vect_paths.items():
            vds = gdal.OpenEx(vds_path, gdal.OF_UPDATE | gdal.OF_VECTOR)

            logger.info(f'Processing zonal stats for raster {rds_id} and {vds_id} ')
            srf = os.path.join('/data/sids/tmp/test', f'{vds_id}_{rds_id}_stats.json')
            if not os.path.exists(srf) or os.path.getsize(srf) == 0:

                stat_result = zonal_stats(raster_path_or_ds=stdz_rds, vector_path_or_ds=vds, band=band)
                with open(srf, 'w') as out:
                    json.dump(stat_result, out)

            else:
                with open(srf) as infl:
                    stat_result = json.load(infl)




            #1 add new column to in mem shp

            add_field_to_vector(src_ds=vds, stats_dict=stat_result, field_name=rds_id)
            #prepare out folders for json and mvt
            vector_json_dir = os.path.join(out_vector_path, root_geojson_folder_name)
            if not os.path.exists(vector_json_dir):
                util.mkdir_recursive(vector_json_dir)



            if not store_attrs_per_vector:
                #1 convert vds to GeoJSON as rds_id/vds_id.json
                vector_geojson_path = os.path.join(vector_json_dir, f'{rds_id}_{vds_id}.geojson')
                if os.path.exists(vector_geojson_path):
                    os.remove(vector_geojson_path)


                geojson_opts = [
                    '-f GeoJSON',
                    '-addfields',
                    '-overwrite',
                    '-preserve_fid',
                    '-nlt MULTIPOLYGON',
                    f'-nln {rds_id}',
                    '-skipfailures',
                    '-overwrite'

                ]
                geojson_vds = gdal.VectorTranslate(destNameOrDestDS=vector_geojson_path, srcDS=vds,
                                                   options=' '.join(geojson_opts)
                                                   )
                geojson_vds = None
                #3 export geoJSON to MVT using tippecanoe

                out_mvt_dir_path = os.path.join(out_vector_path, root_mvt_folder_name, rds_id)
                if not os.path.exists(out_mvt_dir_path):
                    util.mkdir_recursive(out_mvt_dir_path)

                res = util.run_tippecanoe(src_geojson_file=vector_geojson_path, layer_name=vds_id,
                                          minzoom=0, maxzoom=12,
                                          output_mvt_dir_path=out_mvt_dir_path
                                          )


                #3 remove the new column from vds
                remove_filed_from_vector(src_ds=vds,field_name=rds_id)

                # add to results for upload
                if rds_id in mvt_folder_paths:
                    mvt_folder_paths[rds_id].append(res)
                else:
                    mvt_folder_paths[rds_id] = [res]
            else:

                if no_proc_rast == n_rast_to_process - 1:

                    logger.info(f'Exporting accumulated {vds_id} to MVT ')
                    #1 last raster, convert the vector to geojson


                    vector_geojson_path = os.path.join(vector_json_dir, f'{vds_id}.geojson')

                    geojson_opts = [
                        '-f GeoJSON',
                        '-addfields',
                        '-overwrite',
                        '-preserve_fid',
                        '-nlt MULTIPOLYGON',
                        f'-nln {vds_id}',
                        '-skipfailures',
                        '-overwrite'


                    ]
                    if os.path.exists(vector_geojson_path):
                        os.remove(vector_geojson_path)

                    geojson_vds = gdal.VectorTranslate(destNameOrDestDS=vector_geojson_path, srcDS=vds,
                                         options=' '.join(geojson_opts),

                                         )
                    geojson_vds = None


                    #2 export to MVT
                    out_mvt_dir_path = os.path.join(out_vector_path, root_mvt_folder_name)
                    if not os.path.exists(out_mvt_dir_path):
                        util.mkdir_recursive(out_mvt_dir_path)

                    res = util.run_tippecanoe(  src_geojson_file=vector_geojson_path, layer_name=vds_id,
                                                minzoom=0, maxzoom=12,
                                                output_mvt_dir_path=out_mvt_dir_path

                                            )

                    mvt_folder_paths[vds_id] = res
                    #2 deallocate

                    vds = None
        no_proc_rast +=1
        #delete stdz raster
        stdz_rds =None



    #upload mvt to blob
    mvt_root_folder = os.path.join(out_vector_path, root_mvt_folder_name)
    geojson_root_folder = os.path.join(out_vector_path, root_geojson_folder_name)

    shutil.rmtree(geojson_root_folder)


    asyncio.run(util.upload_mvts(
        sas_url=sas_url,
        src_folder=mvt_root_folder,
        dst_blob_name=upload_blob_path,
        timeout=3*60*60 #three hours
        )
    )
    missing_files = missing_az_vectors+missing_az_rasters
    for missing_file in missing_files:
        logger.error(f'{missing_file} could not be found on MS Azure and was not processed')

    if remove_tiles_after_upload:
        shutil.rmtree(mvt_root_folder)





#
# def run(raster_layers_csv_blob=None, vector_layers_csv_blob=None,
#         sas_url=None,
#         store_attrs_per_vector=True,
#         out_vector_path=None,
#         vector_format={'ESRI Shapefile':'.shp'},
#         dst_srs=3857
#         ):
#
#     assert out_vector_path not in ('', None), f'Invalid out_vector_path={out_vector_path}'
#
#     assert  vector_format, f'Invalid vector_format={vector_format}. Valid options are {util.SUPPORTED_FORMATS} '
#
#     vformat, ext = next(iter(vector_format.items()))
#
#     assert vformat  in util.SUPPORTED_FORMATS, f'Invalid format={vector_format}. Valid options are {util.SUPPORTED_FORMATS}'
#
#     per = 'vector' if store_attrs_per_vector else 'raster'
#
#     logger.info(f'Going to store vector tiles per {per}')
#
#     wmercP = osr.SpatialReference()
#     wmercP.ImportFromEPSG(dst_srs)
#
#     # make a sync container client
#     with ContainerClient.from_container_url(container_url=sas_url) as c:
#         vector_datasets = dict()
#         vector_csv_stream = c.download_blob(vector_layers_csv_blob)
#
#         with io.BytesIO() as vstr:
#             vector_csv_stream.readinto(vstr)
#             vstr.seek(0)
#             vlines = (line.decode('utf-8') for line in vstr.readlines())
#             vreader = csv.DictReader(vlines)
#
#             for csv_vector_row in vreader:
#
#
#                 vid = csv_vector_row['vector_id']
#                 if '\\' in csv_vector_row['file_name']:
#                     vfile_name = csv_vector_row['file_name'].replace('\\', '/')
#                 else:
#                     vfile_name = csv_vector_row['file_name']
#                 if '\\' in csv_vector_row['path']:
#                     vpath = csv_vector_row['path'].replace('\\', '/')
#                 else:
#                     vpath = csv_vector_row['path']
#
#                 # src_vector_blob_path = os.path.join('/vsiaz/sids/rawdata', vpath, vfile_name)
#                 src_vector_blob_path = os.path.join('rawdata', vpath.replace('Shapefile', 'Shapefiles'), vfile_name)
#
#                 if out_vector_path not in ('', None):
#                     vect_path = os.path.join(out_vector_path, 'in')
#                 else:
#                     vect_path = f'/vsimem/{vfile_name}'
#
#                 #TODO do not forget to set vecpath ot vsimem
#
#                 vds, prj = util.fetch_az_shapefile(  blob_path=src_vector_blob_path,
#                                                 client_container=c,
#                                                 alternative_path='/data/sids/tmp/test/in')
#
#                 #print_field_names(vds,'aaa')
#                 vector_datasets[vid] = vds, prj
#
#                 break
#
#         nrp = 0
#         # fetch a stream
#         rast_csv_stream = c.download_blob(raster_layers_csv_blob)
#         # push the binary stream into RAM
#         with io.BytesIO() as rstr:
#             rast_csv_stream.readinto(rstr)
#             # need to position at the beginning
#             rstr.seek(0)
#             # cretae a generator of text lines from the binary lines
#             rlines = (line.decode('utf-8') for line in rstr.readlines())
#             # instantitae  a reader
#             rreader = csv.DictReader(rlines)
#             for raster_csv_row in rreader:
#
#                 rid = raster_csv_row['attribute_id']
#                 band = int(raster_csv_row['band'])
#                 if '\\' in raster_csv_row['file_name']:
#                     rfile_name = raster_csv_row['file_name'].replace('\\', '/')
#                 else:
#                     rfile_name = raster_csv_row['file_name']
#
#                 if '\\' in raster_csv_row['path']:
#                     rpath = raster_csv_row['path'].replace('\\', '/')
#                 else:
#                     rpath = raster_csv_row['path']
#
#                 src_raster_blob_path = os.path.join('/vsiaz/sids/', rpath, rfile_name)
#                 logger.debug(f'Processing {src_raster_blob_path}')
#                 # standardize
#                 #TODO remove alternative_path
#                 stdz_ds = standardize(src_blob_path=src_raster_blob_path,band=band, alternative_path='/data/sids/tmp/test')
#                 #iterate over vectors and
#                 for vds_id, t in vector_datasets.items():
#                     vds, src_prj = t
#
#                     srf = os.path.join('/data/sids/tmp/test', f'{rid}_st.json')
#
#                     if not os.path.exists(srf) or os.path.getsize(srf) == 0:
#
#                         stat_result = zonal_stats(raster_path_or_ds=stdz_ds, vector_path_or_ds=vds, band=band)
#                         with open(srf, 'w') as out:
#                             json.dump(stat_result, out)
#
#                     else:
#                         with open(srf) as infl:
#                             stat_result = json.load(infl)
#
#
#                     lname = rid
#
#
#                     if store_attrs_per_vector:
#                         logger.debug(f'Storing attrs per vector')
#                         web_merc_vect_path = f'{os.path.join("/vsimem", vds_id)}{ext}'
#
#                         try:
#                             logger.info(f'Attempting to open {web_merc_vect_path}')
#                             web_merc_ds =  gdal.OpenEx(web_merc_vect_path, gdal.OF_UPDATE|gdal.OF_VECTOR)
#
#                         except Exception as e:
#                             logger.info(f'Creating layer {lname} in {web_merc_vect_path}')
#
#                             opts = gdal.VectorTranslateOptions(format=vformat,
#                                                                dstSRS=wmercP.ExportToWkt(),
#                                                                srcSRS=src_prj.ExportToWkt(),
#                                                                reproject=True,
#                                                                addFields=True,
#                                                                # datasetCreationOptions=[],
#                                                                # layerCreationOptions=['COORDINATE_PRECISION=5',
#                                                                #                       'RFC7946=YES'],
#                                                                layerName=lname,
#                                                                skipFailures=True,
#                                                                )
#                             flist = [e for e in vds.GetFileList() if ".shp" in e]
#                             if flist:
#                                 fn = flist[0]
#                                 logger.info(f'Reprojecting {fn} to {web_merc_vect_path}')
#                             # reproject
#                             web_merc_ds = gdal.VectorTranslate(destNameOrDestDS=web_merc_vect_path, srcDS=vds,
#                                                                options=opts)
#
#                         add_field_to_vector(src_ds=web_merc_ds, stats_dict=stat_result, field_name=rid)
#
#                     else:
#                         logger.debug('Storing attrs per raster')
#                         web_merc_vect_path_json = f'{os.path.join(out_vector_path or "/vsimem", rid, vds_id)}.json'
#                         web_merc_vect_path = f'{os.path.join("/vsimem", rid, vds_id)}{ext}'
#
#                         basedir = os.path.dirname(web_merc_vect_path_json)
#                         if not os.path.exists(basedir):
#                             util.mkdir_recursive(basedir)
#                         try:
#
#                             src_ds =  gdal.OpenEx(web_merc_vect_path, gdal.OF_UPDATE|gdal.OF_VECTOR)
#                             driver = src_ds.GetDriver()
#                             driver.Delete(web_merc_vect_path)
#                             src_ds = None
#                             logger.info(f'Removed {web_merc_vect_path}')
#                         except Exception as e:
#
#                             pass
#
#                         logger.info(f'Creating layer {lname} in {web_merc_vect_path}')
#
#                         opts = gdal.VectorTranslateOptions(format=vformat,
#                                                            dstSRS=wmercP.ExportToWkt(),
#                                                            srcSRS=src_prj.ExportToWkt(),
#                                                            reproject=True,
#                                                            addFields=True,
#                                                            # datasetCreationOptions=[],
#                                                            # layerCreationOptions=['COORDINATE_PRECISION=5',
#                                                            #                       'RFC7946=YES'],
#                                                            layerName=lname,
#                                                            skipFailures=True,
#                                                            )
#                         logger.info(f'Reprojecting {vds.GetFileList()} to {web_merc_vect_path}')
#                         # reproject
#                         web_merc_ds = gdal.VectorTranslate(destNameOrDestDS=web_merc_vect_path, srcDS=vds,
#                                                            options=opts)
#
#
#
#
#                         add_field_to_vector(src_ds=web_merc_ds, stats_dict=stat_result, field_name=rid)
#                         web_merc_ds_json = gdal.VectorTranslate(destNameOrDestDS=web_merc_vect_path_json, srcDS=web_merc_ds,
#                                                             format='GeoJSON'
#                                                            )
#                         web_merc_ds_json = None
#                         web_merc_ds = None
#
#                 nrp+=1
#
#
#                 logger.info(f'Finished processing raster id {rid}')
#                 if nrp == 3:
#                     break
#
#         for vds_id, t in vector_datasets.items():
#             vds, prj = t
#             if store_attrs_per_vector:
#                 web_merc_vect_path = f'{os.path.join("/vsimem", vds_id)}{ext}'
#                 web_merc_vect_path_json = f'{os.path.join(out_vector_path, vds_id)}.json'
#                 basedir = os.path.dirname(web_merc_vect_path_json)
#                 if not os.path.exists(basedir):
#                     util.mkdir_recursive(basedir)
#                 web_merc_ds_json = gdal.VectorTranslate(destNameOrDestDS=web_merc_vect_path_json, srcDS=web_merc_vect_path,
#                                                     format='GeoJSON'
#                                                     )
#
#                 web_merc_ds_json = None
#
#             if vds:
#                 fpl = vds.GetFileList()
#                 vds = None
#                 prj = None
#                 if fpl:
#                     for fp in fpl:
#                         if 'vsimem' in fp:
#                             logger.info(f'Unlinking {fp}')
#                             gdal.Unlink(fp)
#

def print_field_names(src_ds, aname):
    """
    Prinf some info about a dataset
    :param src_ds:
    :param aname:
    :return:
    """
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


    import argparse as ap

    arg_parser = ap.ArgumentParser(formatter_class=ap.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument('-rblob', '--raster_layers_csv_blob', type=str, required=True,
                            help='relative path(in respect to the container) of the CSV \
            file that holds info in respect to vector layers', )
    arg_parser.add_argument('-vblob', '--vector_layers_csv_blob', type=str, default=None,
                            help='relative path (in respect to the container) if the CSV\
                            file that contains info related to the raster files to be processed', )
    arg_parser.add_argument('-surl', '--sas_url', default=None,
                            help='MS Azure SAS url granting rw access to the container',
                            type=str)
    arg_parser.add_argument('-ovp', '--out_vector_path',
                            help='abs path to a folder where output data (MVT and JSON) is going to be stored', type=str
                            )
    arg_parser.add_argument('-ublob', '--upload_blob_path',
                            help='relative path (to the container) where the MVT data will be copied', type=str,
                            )

    arg_parser.add_argument('-pvect', '--store_attrs_per_vector',
                            help='relative path (to the container) where the MVT data will be copied', type=str,
                            )

    run(
        raster_layers_csv_blob='config/attribute_list_updated.csv',
        vector_layers_csv_blob='config/vector_list.csv',
        sas_url=sas_url,
        out_vector_path='/data/sids/tmp/test/out/',
        upload_blob_path='vtiles',
        remove_tiles_after_upload=True,

       )
    '''
    :param raster_layers_csv_blob: str, relative path(in respect to the container) of the CSV
            file that holds info in respect to vector layers
    :param vector_layers_csv_blob:str, relative path (in respect to the container) if the CSV
            file that contains info related to the raster files to be processed
    :param sas_url: str, MS Azure SAS surl granting rw access to the container
    :param store_attrs_per_vector: bool, default=??? determines if the zonal statistics
            will be accumulated into the vector layers as columns in the attr table. If False,
            a new vector layer/vector tile will be created for every combination of raster and vector
            layers

    :param out_vector_path:str, abs path to a folder where output data (MVT) is going to be stored
    :param upload_blob_path: str, relative path (to the container) where the MVT data will be copied

    :param remove_tiles_after_upload: bool, default=False, if True the MVT data will be removed
    :param root_mvt_folder_name: str, the name of the folder where all MVT's will be stored inside the
            out_vector_path folder
    :param root_geojson_folder_name: str, the name of the folder where the GeoJSON datat will be written
            inside the out_vector_path folder
    :param alternative_path: str, full path to a folder where the incoming thata that is downlaoded will be stored and
            cached. Used during developemnt
    '''






