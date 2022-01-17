import os
import logging
import time
_, name = os.path.split(__file__)
logger = logging.getLogger(name)
from osgeo import gdal, gdalconst, osr


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



def fetch_vector_from_azure(blob_path=None, client_container=None, ):

    logger.info(f'Fetching {blob_path} from {client_container.container_name} container and')

    # if not 'vsimem' in dst_vect_path:
    #     if os.path.exists(dst_vect_path):
    #         logger.info(f'Reading {blob_path} from {dst_vect_path}')
    #         return gdal.OpenEx(dst_vect_path, gdalconst.OF_VECTOR | gdalconst.OF_READONLY)

    name = os.path.split(blob_path)[-1]
    root = os.path.splitext(blob_path)[0]
    rroot, ext = os.path.splitext(name)
    read_path = f'/vsimem/{rroot}{ext}'

    try:
        dst_vect_path = read_path.replace('.shp', '.geojson')
        vds = gdal.OpenEx(dst_vect_path, gdalconst.OF_VECTOR | gdalconst.OF_UPDATE)
    except Exception as eee:
        logger.info(f'Could not fetch {dst_vect_path} from mem')

    for e in ('.shp', '.shx', '.dbf', '.prj'):
        vsi_pth = f'/vsimem/{rroot}{e}'
        remote_pth = f'{root}{e}'
        strm = client_container.download_blob(remote_pth)
        v = strm.readall()
        gdal.FileFromMemBuffer(vsi_pth, v)
    vds = gdal.OpenEx(read_path, gdalconst.OF_VECTOR | gdalconst.OF_UPDATE)
    # if 'Shapefile' in vds.GetDriver().LongName:
    #     logger.info(f'Converting {read_path} to GeoJSON')
    #     lname = name.replace('.shp', '.geojson')
    #     dst_vect_path = read_path.replace('.shp', '.geojson')
    #     json_vds = gdal.VectorTranslate(destNameOrDestDS=dst_vect_path,
    #                                     srcDS=vds,
    #                                     layerName=lname,
    #                                     format='GeoJSON')
    #     vds = None
    #     gdal.Unlink(read_path)
    #
    #     return json_vds
    # else:
    return vds

    #return gdal.VectorTranslate(destNameOrDestDS=dst_vect_path, srcDS=vds, layerName=blob_name)


    # if not dst_vect_path in ('', None) :
    #     blob_name = os.path.split(blob_path)[-1]
    #     logger.info(f'Creating {dst_vect_path}')
    #     new_ds = gdal.VectorTranslate(destNameOrDestDS=dst_vect_path, srcDS=vds, layerName=blob_name)
    #     new_ds = None
    # return vds



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
            ds = gdal.OpenEx(dst_shp_path, gdalconst.OF_VECTOR|gdalconst.OF_READONLY)
            prjf = dst_shp_path.replace('.shp', '.prj' )
            if os.path.exists(prjf) and ds.GetSpatialRef() is None:
                sr = osr.SpatialReference()
                sr.ImportFromWkt(open(prjf).read())
            return ds, sr
            #return ogr.Open(dst_shp_path, gdal.OF_VECTOR|gdal.OF_READONLY)
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
    #make sure the prj is loaded
    prj = osr.SpatialReference()
    prjf = read_path.replace('.shp', '.prj')
    stat = gdal.VSIStatL(prjf, gdal.VSI_STAT_SIZE_FLAG)
    vsifile = gdal.VSIFOpenL(prjf, 'r')  # could also use memds.GetDescription() instead of vsipath var
    prjtxt = gdal.VSIFReadL(1, stat.size, vsifile).decode('utf-8')
    prj.ImportFromWkt(prjtxt)
    gdal.VSIFCloseL(vsifile)

    #this code produces sigsegv!!!!!!!!!!!!!
    #l = vds.GetLayer(0)

    # for feature in l:
    #     geom = feature.GetGeometryRef()
    #     ng = geom.Buffer(0.0)
    #     feature.SetGeometry(ng)
    #     l.SetFeature(feature)
    #     #print(feature.GetFID(), feature.GetGeometryRef().GetEnvelope())
    # l.ResetReading()

    # for feature in l:
    #     if feature.GetField('NAME_0') == 'Kiribati':
    #         l.DeleteFeature(feature.GetFID())
    #     print(feature.GetFID(), feature.GetField('NAME_0'), feature.GetGeometryRef().GetEnvelope())
    # l.ResetReading()

    # print(l.GetName())
    # sr = osr.SpatialReference()
    # sr.ImportFromEPSG(4326)
    # lc = vds.CopyLayer(l, f'{l.GetName()}_cp', options=[f'DST_SRSWKT={sr.ExportToWkt()}'])


    if alternative_path is not None:
        assert os.path.exists(alternative_path), f'alternative_path={alternative_path} does not exist'
        dst_shp_path = os.path.join(alternative_path, blob_name)
        logger.info(f'Creating {dst_shp_path}')
        new_ds = gdal.VectorTranslate(destNameOrDestDS=dst_shp_path, srcDS=vds, layerName=blob_name, srcSrs=prjtxt)
        new_ds = None
    return vds, prj

