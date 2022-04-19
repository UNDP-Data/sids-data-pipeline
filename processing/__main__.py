import asyncio
import csv
import json
import os
import shutil

from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import urlparse

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient
from osgeo import gdal, ogr

from . import utils
from .azblob import HandyContainerClient, upload_mvts
from .standardization import standardize
from .utils import config, logging, cwd
from .zonal_stats import zonal_stats

load_dotenv()
gdal.UseExceptions()
logger = logging.getLogger(__name__)


def config_bool(val):
    config[val].lower() in ('yes', 'on', 'true', '1')


aggregate_vect = config_bool('aggregate_vect')
alternative_path = Path(config['cache_folder'])
debug = config_bool('debug')
filter_rid = config['raster_id'].split(',')
filter_vid = config['vector_id'].split(',')
out_vector_path = Path(config['out_vector_path'])
raster_layers_csv_blob = config['raster_layers_csv_blob']
remove_tiles_after_upload = config_bool('remove_tiles_after_upload')
sas_url = config['sas_url'] or os.environ.get('SAS_SIDS_CONTAINER', None)
upload_blob_path = config['upload_blob_path']
vector_layers_csv_blob = config['vector_layers_csv_blob']
root_mvt_folder_name = 'tiles'
root_geojson_folder_name = 'json'


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


def add_field_to_vector(src_ds=None, stats_dict=None, stat_func='mean', field_name=None):
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
    field_names = [ldef.GetFieldDefn(i).GetName()
                   for i in range(ldef.GetFieldCount())]
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
    # TODO remove next line 100%
    sd = {int(k): v for (k, v) in stats_dict.items()}
    for feat in layer:
        fid = feat.GetFID()
        try:
            v = sd[fid][stat_func]
            feat.SetField(field_name, v)
            layer.SetFeature(feat)
        except Exception as e:
            logger.warning(
                f'No {field_name} value was assigned to feature {fid} {e} ')
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
    field_names = [ldef.GetFieldDefn(i).GetName()
                   for i in range(ldef.GetFieldCount())]
    flist = [e for e in src_ds.GetFileList() if ".shp" in e]
    if flist:
        fn = f'{flist[0]}'
    else:
        fn = ''
    if field_name in field_names:
        logger.info(f'Removing field {field_name} from {fn}')
        field_id = [i for i in range(ldef.GetFieldCount()) if ldef.GetFieldDefn(
            i).GetName() == field_name].pop(0)
        layer.DeleteField(field_id)
    layer = None


def download_paths(blob):
    output_csv = cwd / f'../inputs/{blob}'
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with ContainerClient.from_container_url(container_url=sas_url) as c:
        with open(output_csv, 'wb') as f:
            f.write(c.download_blob(blob).readall())


def get_paths(blob, id_name, filter_id, prefix):
    output_csv = cwd / f'../inputs/{blob}'
    with open(output_csv) as f:
        rows = list(csv.DictReader(f))

    all_ids = sorted(set([e[id_name] for e in rows]))
    if not set(filter_id).issubset(all_ids):
        exc = f'Invalid id "{", ".join(filter_id)}". Valid values are "{", ".join(all_ids)}"'
        raise Exception(exc)
    rows = list(filter(lambda x: x[id_name] in filter_id, rows))

    for row in rows:
        row['file_name'] = row['file_name'].replace('\\', '/')
        row['path'] = row['path'].replace('\\', '/')
        row['blob_path'] = Path(prefix, row['path'], row['file_name'])
        row['cache_path'] = Path(cwd, '../inputs', prefix, row['path'])
        row['band'] = int(row.get('band', 0))
    return rows


def download_data(rows, id_name):
    paths = {}
    with ContainerClient.from_container_url(container_url=sas_url) as c:
        for row in rows:
            try:
                blob_list = c.list_blobs(name_starts_with=row['blob_path'])
                blob_paths = [e.name for e in blob_list]
                if not blob_paths:
                    raise ResourceNotFoundError(row['blob_path'])
                del blob_list
                utils.fetch_vector_from_azure(
                    rel_blob_path=row['blob_path'],
                    client_container=c,
                    alternative_path=row['cache_path']
                )
            except ResourceNotFoundError:
                raise ResourceNotFoundError(row['blob_path'])
            paths[row[id_name]] = row['cache_path'] / row['file_name']
    if len(paths) == 0:
        logger.warning('No files are going to be processed. Going to exit.')
        exit()
    return paths


def get_vector_data():
    download_paths(vector_layers_csv_blob)
    rows = get_paths(vector_layers_csv_blob,
                     'vector_id', filter_vid, 'rawdata')
    paths = download_data(rows, 'vector_id')
    return paths


def get_raster_data():
    download_paths(raster_layers_csv_blob)
    rows = get_paths(raster_layers_csv_blob, 'attribute_id', filter_rid, '')
    paths = download_data(rows, 'attribute_id')
    return paths


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
    field_names = [ldef.GetFieldDefn(i).GetName()
                   for i in range(ldef.GetFieldCount())]
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


def set_env_azure(debug):
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                                             "%Y-%m-%d %H:%M:%S"))
    azlogger = logging.getLogger(
        'azure.core.pipeline.policies.http_logging_policy')
    azlogger.setLevel(logging.WARNING)

    rlogger = logging.getLogger()
    rlogger.handlers.clear()
    rlogger.addHandler(sthandler)
    rlogger.name = Path(__file__).name
    if debug:
        rlogger.setLevel(logging.DEBUG)
    else:
        rlogger.setLevel(logging.INFO)

    sas_url = os.environ.get('SAS_SIDS_CONTAINER', None)
    parsed = urlparse(sas_url)
    azure_storage_account = parsed.netloc.split('.')[0]
    azure_sas_token = parsed.query

    os.environ['AZURE_STORAGE_ACCOUNT'] = azure_storage_account
    os.environ['AZURE_STORAGE_SAS_TOKEN'] = azure_sas_token
    os.environ['AZURE_SAS'] = azure_sas_token


def set_env_gdal(debug):
    os.environ['CPL_TMPDIR'] = '/tmp'
    os.environ['GDAL_CACHEMAX'] = '1000'
    os.environ['GDAL_DISABLE_READDIR_ON_OPEN'] = 'TRUE'
    os.environ['GDAL_HTTP_MERGE_CONSECUTIVE_RANGES'] = 'YES'
    os.environ['GDAL_HTTP_MULTIPLEX'] = 'YES'
    os.environ['GDAL_HTTP_TIMEOUT'] = '3600'  # seconds
    os.environ['GDAL_HTTP_UNSAFESSL'] = 'YES'
    os.environ['GDAL_HTTP_VERSION'] = '2'
    os.environ['GDAL_NUM_THREADS'] = 'ALL_CPUS'
    os.environ['VSI_CACHE'] = 'TRUE'
    os.environ['VSI_CACHE_SIZE'] = '5000000'  # 5 MB (per file-handle)
    if debug:
        os.environ['CPL_DEBUG'] = 'ON'


if __name__ == '__main__':
    logger.info('starting')
    set_env_azure(debug)
    set_env_gdal(debug)
    vector_paths = get_vector_data()
    raster_paths = get_raster_data()

    failed = list()
    assert raster_layers_csv_blob not in ('')
    assert out_vector_path not in (
        '', None), f'Invalid out_vector_path={out_vector_path}'
    if aggregate_vect:
        logger.info(f'Running in aggregate mode')
    for rds_id, tp in raster_paths.items():
        vsiaz_rds_path, band = tp
        # 1 STANDARDIZE
        try:
            stdz_rds = standardize(
                src_blob_path=vsiaz_rds_path, band=band, alternative_path=alternative_path)
        except Exception as stdze:
            logger.error(
                f'Failed to standardize {vsiaz_rds_path}:{band}. \n {stdze}. Skipping')
            failed.append(vsiaz_rds_path)
            continue
        for vds_id, vds_path in vector_paths.items():
            vds = gdal.OpenEx(vds_path, gdal.OF_UPDATE | gdal.OF_VECTOR)
            logger.info(
                f'Processing zonal stats for raster {rds_id} and vector {vds_id} ')
            try:
                if not alternative_path:
                    stat_result = zonal_stats(
                        raster_path_or_ds=stdz_rds, vector_path_or_ds=vds, band=band)
                else:
                    srf = os.path.join(
                        alternative_path, f'{vds_id}_{rds_id}_stats.json')
                    if not os.path.exists(srf) or os.path.getsize(srf) == 0:
                        stat_result = zonal_stats(
                            raster_path_or_ds=stdz_rds, vector_path_or_ds=vds, band=band)
                        with open(srf, 'w') as out:
                            json.dump(stat_result, out)
                    else:
                        logger.info(f'Reusing zonal stats from {srf}')
                        with open(srf) as infl:
                            stat_result = json.load(infl)
            except Exception as zse:
                logger.error(
                    f'Failed to compute zonal stats for {vsiaz_rds_path}:{band} <> {vds_path}. \n {zse}. Skipping')
                failed.append(vsiaz_rds_path)
                continue
            # 1 add new column to in mem shp
            try:
                add_field_to_vector(
                    src_ds=vds, stats_dict=stat_result, field_name=rds_id)
            except Exception as afe:
                logger.error(
                    f'Failed to add filed {rds_id} to vector {vds_path}.Skipping!')
                continue
            # prepare out folders for json and mvt
            vector_json_dir = os.path.join(
                out_vector_path, root_geojson_folder_name)
            if not os.path.exists(vector_json_dir):
                utils.mkdir_recursive(vector_json_dir)
            if not aggregate_vect:
                # 1 convert vds to GeoJSON as rds_id/vds_id.json
                vector_geojson_path = os.path.join(
                    vector_json_dir, f'{rds_id}_{vds_id}.geojson')
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
                                                       options=' '.join(
                                                           geojson_opts)
                                                       )
                    geojson_vds = None
                except Exception as geojson_translate_error:
                    logger.error(
                        f'Failed to convert {vds_path} to GeoJSON. {geojson_translate_error}. The layer will not be exported!')
                    continue
                # 3 export geoJSON to MVT using tippecanoe
                out_mvt_dir_path = os.path.join(
                    out_vector_path, root_mvt_folder_name, rds_id)
                if not os.path.exists(out_mvt_dir_path):
                    utils.mkdir_recursive(out_mvt_dir_path)
                try:
                    res = utils.export_with_tippecanoe(src_geojson_file=vector_geojson_path, layer_name=vds_id,
                                                       minzoom=0, maxzoom=12,
                                                       output_mvt_dir_path=out_mvt_dir_path
                                                       )
                except Exception as mvt_translate_error:
                    logger.error(
                        f'Failed to convert {vector_geojson_path} to MVT. {mvt_translate_error}. The layer will not be exported!')
                    continue
                # 4 upload
                upload_folder = os.path.join(
                    out_vector_path, root_mvt_folder_name)
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
                # 5 remove the new column from vds
                remove_field_from_vector(src_ds=vds, field_name=rds_id)
                # 5 remove the tile layer and geojson
                if remove_tiles_after_upload:
                    logger.info(
                        f'Removing GeoJSON and MVT layers for {rds_id}/{vds_id}')
                    shutil.rmtree(upload_folder)
                shutil.rmtree(vector_json_dir)
        # delete stdz raster
        stdz_rds = None
    # handle aggregated
    if aggregate_vect:
        vector_json_dir = os.path.join(
            out_vector_path, root_geojson_folder_name)
        if not os.path.exists(vector_json_dir):
            utils.mkdir_recursive(vector_json_dir)
        out_mvt_dir_path = os.path.join(out_vector_path, root_mvt_folder_name)
        if not os.path.exists(out_mvt_dir_path):
            utils.mkdir_recursive(out_mvt_dir_path)
        for vds_id, vds_path in vector_paths.items():
            vds = gdal.OpenEx(vds_path, gdal.OF_UPDATE | gdal.OF_VECTOR)
            logger.info(f'Exporting accumulated {vds_id} to MVT ')
            vector_geojson_path = os.path.join(
                vector_json_dir, f'{vds_id}.geojson')
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
            except Exception as agg_geojson_translate_error:
                logger.error(
                    f'Failed to convert {vds_path} to GeoJSON. {agg_geojson_translate_error}. The layer {vds_id} will not be exported!')
                continue
            # 2 export to MVT
            try:

                res = utils.export_with_tippecanoe(src_geojson_file=vector_geojson_path, layer_name=vds_id,
                                                   minzoom=0, maxzoom=12,
                                                   output_mvt_dir_path=out_mvt_dir_path)
            except Exception as agg_mvt_translate_error:
                logger.error(
                    f'Failed to convert {vector_geojson_path} to MVT. {agg_mvt_translate_error}. The layer {vds_id} will not be exported!')
                continue
            # upload mvt to blob
            if os.path.exists(out_mvt_dir_path):
                logger.info(
                    f'Going to upload vector tiles from {res} to container {cname}/{upload_blob_path}')
                asyncio.run(
                    upload_mvts(
                        sas_url=sas_url,
                        src_folder=out_mvt_dir_path,
                        dst_blob_name=upload_blob_path,
                        timeout=3*60*60  # three hours
                    )
                )
                if remove_tiles_after_upload:
                    shutil.rmtree(os.path.join(out_mvt_dir_path, vds_id))
            if os.path.exists(vector_json_dir):
                shutil.rmtree(vector_json_dir)
            vds = None
    # report missing and failed
    for fail in failed:
        logger.error(f'{fail} was not processed.')
