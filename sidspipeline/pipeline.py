import logging
import os
from sidspipeline.standardization import standardize
from sidspipeline.azblob import HandyContainerClient, upload_mvts
import io
import csv
import asyncio
from osgeo import gdal, ogr
import json
from azure.storage.blob import ContainerClient
from sidspipeline import util
from sidspipeline.zonal_stats import zonal_stats
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



def remove_field_from_vector(src_ds=None, field_name=None):
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
            aggregate_vect=False,
            out_vector_path=None,
            upload_blob_path=None,
            remove_tiles_after_upload=True,
            # the args below should normally not be changed
            root_mvt_folder_name='tiles',
            root_geojson_folder_name = 'json',
            alternative_path=None,

            filter_rid=None,
            filter_vid=None


    ):



    """

    :param raster_layers_csv_blob: str, relative path(in respect to the container) of the CSV
            file that holds info in respect to vector layers
    :param vector_layers_csv_blob:str, relative path (in respect to the container) if the CSV
            file that contains info related to the raster files to be processed
    :param sas_url: str, MS Azure SAS surl granting rw access to the container
    :param aggregate_vect: bool, default=True determines if the zonal statistics
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
    :param filter_rid: list of str, the id/s of the raster/s to be processed. Acts like a filter

    :param filter_vid: list of str, the id/s of the vector/s to be processed. Acts like a filter
    :param alternative_path: str, full path to a folder where the incoming thata that is downlaoded will be stored and
            cached. Used during development

    :return:
    """
    assert raster_layers_csv_blob not in ('')
    assert out_vector_path not in ('', None), f'Invalid out_vector_path={out_vector_path}'



    if aggregate_vect:
        logger.info(f'Running in aggregate mode')

    vsi_vect_paths = dict()
    vsiaz_rast_paths = dict()





    missing_az_vectors = list()
    missing_az_rasters = list()
    cname = None
    failed = list()
    # make a sync container client
    with ContainerClient.from_container_url(container_url=sas_url) as c:
        cname = c.container_name

        vector_csv_stream = c.download_blob(vector_layers_csv_blob)

        with io.BytesIO() as vstr:
            vector_csv_stream.readinto(vstr)
            vstr.seek(0)
            vlines = (line.decode('utf-8') for line in vstr.readlines())
            vreader = csv.DictReader(vlines)

            for csv_vector_row in vreader:

                vid = csv_vector_row['vector_id']
                if filter_vid and vid not in filter_vid:
                    logger.debug(f'Skipping {vid} (filter)')
                    continue
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
                        rel_blob_path=src_vector_blob_path,
                        client_container=c,
                        alternative_path=alternative_path
                    )
                except ResourceNotFoundError:
                    missing_az_vectors.append(src_vector_blob_path)
                    continue

                vsi_vect_paths[vid] = vsi_vect_path


        # fetch  raster stream
        rast_csv_stream = c.download_blob(raster_layers_csv_blob)
        logger.debug(f'{raster_layers_csv_blob} was successfully downloaded..')
        # push the binary stream into RAM
        with io.BytesIO() as rstr:
            rast_csv_stream.readinto(rstr)
            logger.debug(f'{rstr.tell()} bytes were fetched from {raster_layers_csv_blob}')
            # need to position at the beginning
            rstr.seek(0)

            # create a generator of text lines from the binary lines
            rlines = (line.decode('utf-8') for line in rstr.readlines())

            # instantitae  a reader
            rreader = csv.DictReader(rlines)

            for raster_csv_row in rreader:

                rid = raster_csv_row['attribute_id']
                if filter_rid and rid not in filter_rid:
                    logger.debug(f'Skipping {rid} (filter)')
                    continue
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
                    logger.info(f'{src_raster_blob_path} {e}')
                    missing_az_rasters.append(src_raster_blob_path)
                    continue




    no_proc_rast = 0
    n_rast_to_process = len(vsiaz_rast_paths)
    n_vect_to_process = len(vsi_vect_paths)
    if n_rast_to_process == 0:
        logger.warning(f'No raster files are going to be processed. Going to exit.')
        exit()
    if n_vect_to_process == 0:
        logger.warning(f'No vector files are going to be processed. Going to exit.')
        exit()

    logger.info(f'Going to process {n_rast_to_process} raster file/s and {len(vsi_vect_paths)} vector file/s')

    for rds_id, tp in vsiaz_rast_paths.items():
        vsiaz_rds_path, band = tp
        # 1 STANDARDIZE
        try:
            stdz_rds = standardize(src_blob_path=vsiaz_rds_path, band=band, alternative_path=alternative_path)

        except Exception as stdze:
            logger.error(f'Failed to standardize {vsiaz_rds_path}:{band}. \n {stdze}. Skipping')
            failed.append(vsiaz_rds_path)
            continue
        for vds_id, vds_path in vsi_vect_paths.items():
            vds = gdal.OpenEx(vds_path, gdal.OF_UPDATE | gdal.OF_VECTOR)

            logger.info(f'Processing zonal stats for raster {rds_id} and vector {vds_id} ')
            try:
                if not alternative_path:
                    stat_result = zonal_stats(raster_path_or_ds=stdz_rds, vector_path_or_ds=vds, band=band)
                else:
                    srf = os.path.join(alternative_path, f'{vds_id}_{rds_id}_stats.json')
                    if not os.path.exists(srf) or os.path.getsize(srf) == 0:

                        stat_result = zonal_stats(raster_path_or_ds=stdz_rds, vector_path_or_ds=vds, band=band)
                        with open(srf, 'w') as out:
                            json.dump(stat_result, out)

                    else:
                        logger.info(f'Reusing zonal stats from {srf}')
                        with open(srf) as infl:
                            stat_result = json.load(infl)
            except Exception as zse:
                logger.error(f'Failed to compute zonal stats for {vsiaz_rds_path}:{band} <> {vds_path}. \n {zse}. Skipping')
                failed.append(vsiaz_rds_path)
                continue



            #1 add new column to in mem shp
            try:
                add_field_to_vector(src_ds=vds, stats_dict=stat_result, field_name=rds_id)
            except Exception as afe:
                logger.error(f'Failed to add filed {rds_id} to vector {vds_path}.Skipping!')
                continue
            #prepare out folders for json and mvt
            vector_json_dir = os.path.join(out_vector_path, root_geojson_folder_name)
            if not os.path.exists(vector_json_dir):
                util.mkdir_recursive(vector_json_dir)



            if not aggregate_vect:

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
                logger.info(f'Exporting {vds_path} to {vector_geojson_path}')
                try:
                    geojson_vds = gdal.VectorTranslate(destNameOrDestDS=vector_geojson_path, srcDS=vds,
                                                   options=' '.join(geojson_opts)
                                                   )
                    geojson_vds = None
                except Exception as geojson_translate_error:
                    logger.error(f'Failed to convert {vds_path} to GeoJSON. {geojson_translate_error}. The layer will not be exported!')
                    continue
                #3 export geoJSON to MVT using tippecanoe

                out_mvt_dir_path = os.path.join(out_vector_path, root_mvt_folder_name, rds_id)
                if not os.path.exists(out_mvt_dir_path):
                    util.mkdir_recursive(out_mvt_dir_path)
                try:
                    res = util.export_with_tippecanoe(src_geojson_file=vector_geojson_path, layer_name=vds_id,
                                                  minzoom=0, maxzoom=12,
                                                  output_mvt_dir_path=out_mvt_dir_path
                                                  )


                except Exception as mvt_translate_error:
                    logger.error(
                        f'Failed to convert {vector_geojson_path} to MVT. {mvt_translate_error}. The layer will not be exported!')
                    continue

                #4 upload
                upload_folder = os.path.join(out_vector_path, root_mvt_folder_name)
                logger.info(
                    f'Going to upload vector tiles from {upload_folder} to container {cname}/{upload_blob_path}')
                asyncio.run(
                    upload_mvts(
                        sas_url=sas_url,
                        src_folder=upload_folder,
                        dst_blob_name=f'{upload_blob_path}',
                        timeout=3 * 60 * 60  # three hours
                    )
                )

                #5 remove the new column from vds
                remove_field_from_vector(src_ds=vds, field_name=rds_id)
                #5 remove the tile layer and geojson
                if remove_tiles_after_upload:
                    logger.info(f'Removing GeoJSON and MVT layers for {rds_id}/{vds_id}')
                    shutil.rmtree(upload_folder)
                shutil.rmtree(vector_json_dir)

        #delete stdz raster
        stdz_rds =None


    # handle aggregated
    if aggregate_vect:
        vector_json_dir = os.path.join(out_vector_path, root_geojson_folder_name)
        if not os.path.exists(vector_json_dir):
            util.mkdir_recursive(vector_json_dir)
        out_mvt_dir_path = os.path.join(out_vector_path, root_mvt_folder_name)
        if not os.path.exists(out_mvt_dir_path):
            util.mkdir_recursive(out_mvt_dir_path)

        for vds_id, vds_path in vsi_vect_paths.items():
            vds = gdal.OpenEx(vds_path, gdal.OF_UPDATE | gdal.OF_VECTOR)
            logger.info(f'Exporting accumulated {vds_id} to MVT ')


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

            try:
                logger.info(f'Exporting {vds_path} to  {vector_geojson_path}')
                geojson_vds = gdal.VectorTranslate(destNameOrDestDS=vector_geojson_path, srcDS=vds,
                                     options=' '.join(geojson_opts),

                                     )
                geojson_vds = None
            except Exception as agg_geojson_translate_error:
                logger.error(
                    f'Failed to convert {vds_path} to GeoJSON. {agg_geojson_translate_error}. The layer {vds_id} will not be exported!')
                continue


            #2 export to MVT

            try:

                res = util.export_with_tippecanoe(src_geojson_file=vector_geojson_path, layer_name=vds_id,
                                                  minzoom=0, maxzoom=12,
                                                  output_mvt_dir_path=out_mvt_dir_path

                                                  )
            except Exception as agg_mvt_translate_error:
                logger.error(
                    f'Failed to convert {vector_geojson_path} to MVT. {agg_mvt_translate_error}. The layer {vds_id} will not be exported!')
                continue


            #upload mvt to blob


            if os.path.exists(out_mvt_dir_path):
                logger.info(f'Going to upload vector tiles from {res} to container {cname}/{upload_blob_path}')
                asyncio.run(
                    upload_mvts(
                        sas_url=sas_url,
                        src_folder=out_mvt_dir_path,
                        dst_blob_name=upload_blob_path,
                        timeout=3*60*60 #three hours
                        )
                )
                if remove_tiles_after_upload:
                    shutil.rmtree(os.path.join(out_mvt_dir_path, vds_id))

            if os.path.exists(vector_json_dir):
                shutil.rmtree(vector_json_dir)
            vds = None

    #report missing and failed

    missing_files = missing_az_vectors+missing_az_rasters
    for missing_file in missing_files:
        logger.error(f'{missing_file} could not be found on MS Azure and was not processed.')

    for fail in failed:
        logger.error(f'{fail} was not processed.')





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







def main():

    logging.basicConfig()
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                                             "%Y-%m-%d %H:%M:%S"))


    azlogger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
    azlogger.setLevel(logging.WARNING)




    ##### EXAMPLE ######################
    #csv_config_blob_path = 'config'

    # run(
    #     raster_layers_csv_blob='config/attribute_list_updated.csv',
    #     vector_layers_csv_blob='config/vector_list.csv',
    #     sas_url=sas_url,
    #     out_vector_path='/data/sids/tmp/test/out/',
    #     aggregate_vect=True,
    #     upload_blob_path='vtiles',
    #     remove_tiles_after_upload=True,
    #
    # )
    ##### EXAMPLE ######################

    import argparse as ap
    from urllib.parse import urlparse


    def boolean_string(s):
        if s not in {'False', 'True'}:
            raise ValueError('Not a valid boolean string')
        return s == 'True'


    class HelpParser(ap.ArgumentParser):
        def error(self, message):
            #sys.stderr.write('error: %s\n' % message)
            self.print_help()
            exit(0)

    arg_parser = HelpParser(formatter_class=ap.ArgumentDefaultsHelpFormatter,
                                   description='Run the SIDS data pipeline. The pipeline computes zonal stats for a'
                                               ' number of vector layers from a number of raster layers.\nThe results'
                                               ' are converted into MapBox Vector Tile format and uploaded to an Azure Blob'
                                               ' storage container.\nThe specs for the raster and vector files are fetched'
                                               ' from CSV files stored in same Azure Blob storage.'

                                   )
    arg_parser.add_argument('-rb', '--raster_layers_csv_blob', type=str, required=True,
                            help='relative path(in respect to the container) of the CSV \
            file that holds info in respect to vector layers', )
    arg_parser.add_argument('-vb', '--vector_layers_csv_blob', type=str, default=None,
                            help='relative path (in respect to the container) if the CSV\
                            file that contains info related to the raster files to be processed', )
    arg_parser.add_argument('-su', '--sas_url', default=None,
                            help='MS Azure SAS url granting rw access to the container. Alternatively the environment'
                                 'variable SAS_SIDS_CONTAINER can be used to supply a SAS URL',
                            type=str)
    arg_parser.add_argument('-ov', '--out_vector_path',
                            help='abs path to a folder where output data (MVT and JSON) is going to be stored', type=str
                            )
    arg_parser.add_argument('-ub', '--upload_blob_path',
                            help='relative path (to the container) where the MVT data will be copied', type=str,
                            )

    arg_parser.add_argument('-ag', '--aggregate_vect',
                            help='determines if the zonal statistics will be accumulated into the vector layers as '
                                 'columns in the attr table. If False, a new vector layer/vector tile will be created '
                                 'for every combination of raster and vector layers', type=boolean_string,
                            default=True
                            )
    arg_parser.add_argument('-rm', '--remove_tiles_after_upload',
                            help='if the tiles should be removed after upload', type=boolean_string,
                            default=True
                            )
    arg_parser.add_argument('-cf', '--cache_folder',
                            help='Abs path to a folder where input data can be cached and reread an next launch', type=str
                            )


    arg_parser.add_argument('-rid', '--raster_id',
                            help='the id/s (multiple) of the raster file as defined in raster blob spec csv.\n'
                                 'If provided only the supplied rasters will be processed ',
                            type=str,
                            default=None,
                            action='store',
                            dest='raster_id',
                            nargs='*',

                            )
    arg_parser.add_argument('-vid', '--vector_id',
                            help='the id/s (multiple) of the vector file as defined in vector blob spec csv.\n'
                                 'If provided only the supplied vectors will be processed ',
                            type=str,
                            default=None,
                            action='store',
                            dest='vector_id',
                            nargs='*',

                            )

    arg_parser.add_argument('-d', '--debug',
                            help='debug mode on/off', type=bool,
                            default=False
                            )

    # parse and collect args
    args = arg_parser.parse_args()


    raster_layers_csv_blob = args.raster_layers_csv_blob
    vector_layers_csv_blob = args.vector_layers_csv_blob
    sas_url = args.sas_url
    upload_blob_path = args.upload_blob_path
    remove_tiles_after_upload = args.remove_tiles_after_upload
    out_vector_path = args.out_vector_path
    aggregate_vect = args.aggregate_vect
    debug = args.debug
    alternative_path=args.cache_folder
    rid = args.raster_id
    vid = args.vector_id

    rlogger = logging.getLogger()
    # remove the default stream handler and add the new on too it.
    rlogger.handlers.clear()
    rlogger.addHandler(sthandler)
    if debug:
        os.environ['CPL_DEBUG'] = 'ON'
        rlogger.setLevel(logging.DEBUG)
    else:
        rlogger.setLevel(logging.INFO)
    rlogger.name = os.path.split(__file__)[-1]

    # use env variable SAS_SIDS_CONTAINER
    sas_url = sas_url or os.environ.get('SAS_SIDS_CONTAINER', None)

    parsed = urlparse(sas_url)
    #AZURE_SAS and AZURE_STORAGE_SAS_TOKEN
    azure_storage_account = parsed.netloc.split('.')[0]
    azure_sas_token = parsed.query

    os.environ['AZURE_STORAGE_ACCOUNT'] = azure_storage_account
    os.environ['AZURE_STORAGE_SAS_TOKEN'] = azure_sas_token
    os.environ['AZURE_SAS'] = azure_sas_token
    #GDAL suff
    os.environ['CPL_TMPDIR'] = '/tmp'
    os.environ['GDAL_CACHEMAX'] = '1000'
    os.environ['VSI_CACHE'] = 'TRUE'
    os.environ['VSI_CACHE_SIZE'] = '5000000' # 5 MB (per file-handle)
    os.environ['GDAL_DISABLE_READDIR_ON_OPEN'] = 'TRUE'
    os.environ['GDAL_HTTP_MERGE_CONSECUTIVE_RANGES'] = 'YES'
    os.environ['GDAL_HTTP_MULTIPLEX'] = 'YES'
    os.environ['GDAL_HTTP_VERSION'] = '2'
    os.environ['GDAL_HTTP_TIMEOUT'] = '3600' # secs





    run(
        raster_layers_csv_blob=raster_layers_csv_blob,
        vector_layers_csv_blob=vector_layers_csv_blob,
        sas_url=sas_url,
        out_vector_path=out_vector_path,
        aggregate_vect=aggregate_vect,
        upload_blob_path=upload_blob_path,
        remove_tiles_after_upload=remove_tiles_after_upload,
        alternative_path=alternative_path,

        filter_rid=rid,
        filter_vid=vid


       )


if __name__ == '__main__':
    main()