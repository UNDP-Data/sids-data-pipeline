import os
import logging
import time
_, name = os.path.split(__file__)
logger = logging.getLogger(name)
from osgeo import gdal, gdalconst


import subprocess
SUPPORTED_FORMATS = {
        'ESRI Shapefile': 'shp',
        'MVT':'pbf',
        'GeoJSON':'geojson'
}


def flush_cache(passwd):
    """
    Flush Linux VM caches. Useful for doing meaningful tmei measurements for
    NetCDF or similar libs.
    Needs sudo password
    :return: bool, True if success, False otherwise
    """
    logger.debug('Clearing the OS cache using sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches')
    #ret = os.system('echo %s | sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"' % passwd)
    ret = os.popen('sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"', 'w').write(passwd)
    return not bool(ret)

def timeit(func=None,loops=1,verbose=False, clear_cache=False, sudo_passwd=None):
    #print 0, func, loops, verbose, clear_cache, sudo_passwd
    if func != None:
        if clear_cache:
            assert sudo_passwd, 'sudo_password argument is needed to clear the kernel cache'

        def inner(*args,**kwargs):
            sums = 0.0
            mins = 1.7976931348623157e+308
            maxs = 0.0
            logger.debug('====%s Timing====' % func.__name__)
            for i in range(0,loops):
                if clear_cache:
                    flush_cache(sudo_passwd)
                t0 = time.time()
                result = func(*args,**kwargs)
                dt = time.time() - t0
                mins = dt if dt < mins else mins
                maxs = dt if dt > maxs else maxs
                sums += dt
                if verbose == True:
                    logger.debug('\t%r ran in %2.9f sec on run %s' %(func.__name__,dt,i))
            logger.debug('%r min run time was %2.9f sec' % (func.__name__,mins))
            logger.debug('%r max run time was %2.9f sec' % (func.__name__,maxs))
            logger.info('%r avg run time was %2.9f sec in %s runs' % (func.__name__,sums/loops,loops))
            logger.debug('==== end ====')
            return result

        return inner
    else:
        def partial_inner(func):
            return timeit(func,loops,verbose, clear_cache, sudo_passwd)
        return partial_inner




def scantree(path):
    """Recursively yield sorted DirEntry objects for given directory."""
    for entry in sorted(os.scandir(path), key=lambda entry: entry.name):
        if entry.is_dir(follow_symlinks=False):
            #yield entry
            yield from scantree(entry.path)
        else:
            yield entry

def mkdir_recursive(path):
    """
        make dirs in the path recursively
        :param path:
        :return:
    """

    sub_path = os.path.dirname(path)
    if not os.path.exists(sub_path):
        mkdir_recursive(sub_path)
    if not os.path.exists(path):
        os.mkdir(path)



def fetch_vector_from_azure(blob_path=None, client_container=None,alternative_path=None ):
    """

    :param blob_path:
    :param client_container:
    :param alternative_path: only for devel
    :return:
    """

    name = os.path.split(blob_path)[-1]

    if alternative_path is not None:
        assert os.path.exists(alternative_path), f'alternative_path={alternative_path} does not exist'
        dst_shp_path = os.path.join(alternative_path, name)
        if os.path.exists(dst_shp_path):
            logger.info(f'Reading {blob_path} from {dst_shp_path}')
            return dst_shp_path
        else:
            logger.info(f'{dst_shp_path} does not exist in {alternative_path}. Going to read from {blob_path}')



    root = os.path.splitext(blob_path)[0]
    rroot, ext = os.path.splitext(name)
    read_path = f'/vsimem/{rroot}{ext}'


    try:
        logger.info(f'Attempting to read {read_path} from RAM ... ')
        vds = gdal.OpenEx(read_path, gdalconst.OF_VECTOR | gdalconst.OF_UPDATE)
    except Exception as eee:
        logger.info(f'Could not fetch {read_path} from RAM')
        for e in ('.shp', '.shx', '.dbf', '.prj'):
            vsi_pth = f'/vsimem/{rroot}{e}'
            remote_pth = f'{root}{e}'
            strm = client_container.download_blob(remote_pth)
            v = strm.readall()
            gdal.FileFromMemBuffer(vsi_pth, v)
        vds = gdal.OpenEx(read_path, gdalconst.OF_VECTOR | gdalconst.OF_UPDATE)

    if alternative_path is not None:
        assert os.path.exists(alternative_path), f'alternative_path={alternative_path} does not exist'
        dst_shp_path = os.path.join(alternative_path, name)
        logger.info(f'Creating {dst_shp_path}')
        new_ds = gdal.VectorTranslate(destNameOrDestDS=dst_shp_path, srcDS=vds, layerName=name)
        new_ds = None

    return read_path







def fetch_az_shapefile(blob_path=None, client_container=None, alternative_path=None):
    """
    Download a shapefile from an azure blob path
    :param blob_path: str, relativre path to a .shp file
    :param client_container, instance of instance of azure.storage.blob.ContainerClient
    :param alternative_path, str, a local dir, if provided the vector will be writen there as shp
           and subsequently read from there instead of going to AZ blob
           use for devel
    :return: a v2 GDAl/OGR Dataset with one layer representing the shapefile

    NB; the function will fetch/download the '.shp', '.shx', '.dbf', '.prj'
    files and sore the stream into vsi virtual files


    """
    blob_name = os.path.split(blob_path)[-1]

    #gdal.SetConfigOption('SHAPE_RESTORE_SHX','YES')
    if alternative_path is not None:
        assert os.path.exists(alternative_path), f'alternative_path={alternative_path} does not exist'
        dst_shp_path = os.path.join(alternative_path, blob_name)
        if os.path.exists(dst_shp_path):
            logger.info(f'Reading {blob_path} from {alternative_path}')
            return gdal.OpenEx(dst_shp_path, gdalconst.OF_VECTOR|gdalconst.OF_UPDATE)
        else:
            logger.info(f'{dst_shp_path} does not exist in {alternative_path}')

    logger.info(f'Fetching {blob_path} from {client_container.container_name} container')
    name = os.path.split(blob_path)[-1]
    root = os.path.splitext(blob_path)[0]
    rroot, ext = os.path.splitext(name)
    read_path = f'/vsimem/{rroot}{ext}'


    for e in ('.shp', '.shx', '.dbf', '.prj'):
        vsi_pth = f'/vsimem/{rroot}{e}'
        remote_pth  = f'{root}{e}'
        strm  = client_container.download_blob(remote_pth)
        v = strm.readall()
        gdal.FileFromMemBuffer(vsi_pth, v)
    vds = gdal.OpenEx(read_path, gdalconst.OF_VECTOR | gdalconst.OF_UPDATE)

    if alternative_path is not None:
        assert os.path.exists(alternative_path), f'alternative_path={alternative_path} does not exist'
        dst_shp_path = os.path.join(alternative_path, blob_name)
        logger.info(f'Creating {dst_shp_path}')
        new_ds = gdal.VectorTranslate(destNameOrDestDS=dst_shp_path, srcDS=vds, layerName=blob_name)
        new_ds = None
    return vds

'''

rm -rf out/mvt;docker run --rm -it --name tipecanoe -v /data/sids/tmp/test/:/osm klokantech/tippecanoe tippecanoe /osm/out/admin0.geojson -l admin0 --output-to-directory=/osm/out/mvt 
/usr/bin/docker run --rm -it --name tipecanoe -v /data/sids/tmp/test/:/osm klokantech/tippecanoe tippecanoe /osm/out/admin0.geojson -l admin0 --output-to-directory=/osm/out/mvt 

'''


def run_tippecanoe(src_geojson_file=None, layer_name=None, minzoom=None, maxzoom=None,
                   output_mvt_dir_path=None, work_dir='/work',
                   timeout=600):

    """

    :param src_geojson_file:
    :param layer_name:
    :param minzoom:
    :param maxzoom:
    :param output_mvt_dir_path:
    :param work_dir:
    :return:
    """
    # print('tipecanoe -w=/work -v /data/sids/tmp/test/out:/work klokantech/tippecanoe tippecanoe /work/json/admin0.geojson '
    #       '-l admin0 -e /work/tiles/admin0  -Z 0 -z 12 --allow-existing --no-feature-limit --no-tile-size-limit')

    '''
        to try and make created tiles 
        --mount type=bind,source=/etc/passwd,target=/etc/passwd,readonly --mount type=bind,source=/etc/group,target=/etc/group,readonly -u $(id -u $USER):$(id -g $USER)


    '''

    logger.info(f'Exporting {layer_name} from {src_geojson_file} to MVT')

    if not output_mvt_dir_path.endswith(os.path.sep):
        output_mvt_dir_path = f'{output_mvt_dir_path}/'


    bind_dir = os.path.commonpath([output_mvt_dir_path, src_geojson_file])
    if not bind_dir:
        raise Exception(f'{output_mvt_dir_path} and {src_geojson_file} need to share a common path')
    if not bind_dir.endswith(os.path.sep):
        bind_dir = f'{bind_dir}/'

    rel_out_mvt_dir = output_mvt_dir_path.split(bind_dir)[-1]
    rel_geojson = src_geojson_file.split(bind_dir)[-1]

    container_mvt_dir = os.path.join(work_dir,rel_out_mvt_dir, layer_name)
    container_geojson = os.path.join(work_dir,rel_geojson)

    # existing_mvt_folder = os.path.join(output_mvt_dir_path, layer_name)
    # if os.path.exists(existing_mvt_folder):shutil.rmtree(existing_mvt_folder)

    docker_tipecanoe_cmd =  f'docker run --rm -w {work_dir} --name tipecanoe -v {bind_dir}:{work_dir} klokantech/tippecanoe '
    tippecanoe_cmd =    f'tippecanoe  -l {layer_name} -e {container_mvt_dir} ' \
                        f'-z {maxzoom} -Z {minzoom} --allow-existing --no-feature-limit --no-tile-size-limit -f {container_geojson}'

    cmd = f'{docker_tipecanoe_cmd}{tippecanoe_cmd}'
    # docker_tipecanoe_cmd = f'/usr/bin/docker run --rm  --name tipecanoe klokantech/tippecanoe '
    #docker_tipecanoe_cmd = f'ls -h'

    with subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:

        proc.poll()
        try:
            outs, errs = proc.communicate(timeout=timeout)

        except subprocess.TimeoutExpired:
            logger.error(f'{cmd} has timeout out after {timeout} seconds' )
            proc.kill()
            outs, errs = proc.communicate()
        except Exception as e:
            logger.error(f'{e}')
            raise


        return os.path.join(output_mvt_dir_path, layer_name)








