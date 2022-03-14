import logging
from osgeo import osr, gdal
from azure.storage.blob import ContainerClient
from multiprocessing import cpu_count
import os
import io
import csv

gdal.UseExceptions()
NCPUS = cpu_count()
logger = logging.getLogger(__name__)


def standardize(src_blob_path=None, dst_prj=4326, band=None,
                clip_xmin=-180, clip_xmax=180, clip_ymin=-35, clip_ymax=35, clip_ds=None,
                alternative_path=None, format='GTiff',
                no_cpus=NCPUS, multithread=True,
                assumed_epsg=None

                ):

    logger.info(f'Standardizing {src_blob_path}')


    clip_ymin = os.environ.get('CLIP_YMIN', clip_ymin)
    clip_xmin = os.environ.get('CLIP_XMIN', clip_xmin)
    clip_ymax = os.environ.get('CLIP_YMAX', clip_ymax)
    clip_xmax = os.environ.get('CLIP_XMAX', clip_xmax)

    dst_blob_name = os.path.split(src_blob_path)[-1]

    if not 'tif' in dst_blob_name:
        dst_blob_name = f'{os.path.splitext(dst_blob_name)[0]}.tif'

    just_name, ext = os.path.splitext(dst_blob_name)
    dst_blob_name = f'{just_name}_stdz{ext}'

    if alternative_path:
        assert os.path.exists(alternative_path), f'intermediary_folder={alternative_path} does not exist'
        dst_path = os.path.join(alternative_path, dst_blob_name)
        if os.path.exists(dst_path):
            logger.info(f'Reusing {dst_path} instead of {src_blob_path}')
            return gdal.OpenEx(dst_path)



    dst_path = f'/vsimem/{dst_blob_name}'

    resampling = gdal.GRA_NearestNeighbour


    # Open source dataset
    src_ds = gdal.Open(src_blob_path)
    dst_bands = band or src_ds.RasterCount
    src_srs = src_ds.GetSpatialRef()


    if src_srs is None:
        # use the Dataset.GetProjection()
        src_srs = osr.SpatialReference()
        src_srs.ImportFromWkt(src_ds.GetProjection() or src_ds.GetProjectionRef())

    if src_srs is None:
        logger.warning(f'Assuming {src_blob_path} features EPSG:{dst_prj} projection' )
        src_srs = osr.SpatialReference()
        src_srs.ImportFromEPSG(dst_prj)



    #bands
    band_list = [e + 1 for e in range(dst_bands)]

    # Define target SRS
    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromEPSG(dst_prj)
    #check if the input needs to be reprojected or clipped
    try:

        proj_are_equal = int(src_srs.GetAuthorityCode(None)) == int(dst_srs.GetAuthorityCode(None))
    except Exception as evpe:
        logger.info(f'Failed to compare src and dst projections using {osr.SpatialReference.GetAuthorityCode}. Trying using {osr.SpatialReference.IsSame} \n {evpe}')
        try:
            proj_are_equal = bool(src_srs.IsSame(dst_srs))
        except Exception as evpe1:
            logger.info(
                f'Failed to compare src and dst projections using {osr.SpatialReference.IsSame}. Error is \n {evpe}')
            raise  evpe1


    should_reproject = not proj_are_equal



    if should_reproject:
        ce = clip_xmin, clip_ymin, clip_xmax, clip_ymax
        logger.info(f'Reprojecting {src_blob_path} to EPSG:{dst_prj}')


        # gdalwarp  -t_srs '+proj=longlat +datum=WGS84 +pm=-90' -te -242 -30 105 30 -co "TILED=YES"  -co "COMPRESS=LZW"  -wo "NUM_THREADS=ALL_CPUS" -multi -overwrite -r near wildareas-v3-2009-human-footprint.tif wprj.tif
        options = gdal.WarpOptions(format=format, outputBounds=ce,
                                    dstSRS=dst_srs,
                                    creationOptions=['COMPRESS=LZW'],
                                    resampleAlg=resampling,
                                    multithread=multithread,
                                    warpOptions=[f'NUM_THREADS={no_cpus}']
                                   )

        #create a VRT to get the desired bands
        vrt_name = dst_blob_name.replace('.tif', '.vrt')
        vrt_path = f'/vsimem/{vrt_name}'
        src = gdal.Translate(destName=vrt_path, srcDS=src_ds, bandList=band_list,)


        res = gdal.Warp(destNameOrDestDS=dst_path,srcDSOrSrcDSTab=src, options=options)
    else:

        #gdal_translate -r nearest -a_srs EPSG:4326 -projwin_srs '+proj=longlat +datum=WGS84 +pm=0 +over' -projwin -180 30 180 -30 -co "TILED=YES" -co "COMPRESS=LZW" -b 1  GDP_PPP_30arcsec_v3_fixed.tif GDP_PPP_30arcsec_v3.tif

        ce = clip_xmin, clip_ymax, clip_xmax, clip_ymin
        logger.info(f'Clipping {src_blob_path} with bbox: {ce}')
        options = gdal.TranslateOptions(format=format,
                                        creationOptions=['COMPRESS=LZW', "TILED=YES"],
                                        projWin=ce,
                                        #projWinSRS='+proj=longlat +datum=WGS84 +pm=0 +over',
                                        outputSRS=dst_srs,
                                        bandList=band_list,
                                        resampleAlg=resampling)
        res = gdal.Translate(destName=dst_path,srcDS=src_ds, options=options)

    if alternative_path:
        assert os.path.exists(alternative_path), f'alternative_path={alternative_path} does not exist'
        dst_path = os.path.join(alternative_path, dst_blob_name)
        return gdal.Translate(destName=dst_path,srcDS=res)

    return res


def fetch_raster_rows(raster_layers_src_blob=None, sas_url=None):
    """
    Fetch a CSV from Azure Blob containing cfg for SIDS raster layers
    :param raster_layers_src_blob: str, path to the blob containing the file
    :param sas_url: str
    :return: an iterable of rows where each fow is a dict whose keys are
            column names and values are column values
    """
    # make a sync container client
    with ContainerClient.from_container_url(container_url=sas_url) as c:
        #fetch a stream
        rast_cfg_stream = c.download_blob(raster_layers_src_blob)
        #push the binary stream into RAM
        with io.BytesIO() as rstr:
            rast_cfg_stream.readinto(rstr)
            #need to position at the beginning
            rstr.seek(0)
            #cretae a generator of text lines from the binary lines
            rlines = (line.decode('utf-8') for line in rstr.readlines())
            # instanritae  a reader
            rreader = csv.DictReader(rlines)
            # return rows as strings
            return [row for row in rreader]
