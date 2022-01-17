import logging
from osgeo import osr, gdal
from azure.storage.blob import ContainerClient
from multiprocessing import cpu_count
from dotenv import load_dotenv
import os
from data_pipeline.util import timeit
import io
import csv


# from data_pipeline.zonal_stats import zonal_stats
import json
import shapefile
from pygeoprocessing.geoprocessing import zonal_statistics


load_dotenv()
gdal.UseExceptions()

NCPUS = cpu_count()
logger = logging.getLogger(__name__)
#
# def reproject_dataset(dataset=None,band=None, proj_from=None, proj_to=None) -> str:
#     """
#     Reproject a
#     :param dataset:
#     :param band:
#     :param proj_from:
#     :param proj_to:
#     :return:
#     """
#
#     """
#     A sample function to reproject and resample a GDAL dataset from within
#     Python. The idea here is to reproject from one system to another, as well
#     as to change the pixel size. The procedure is slightly long-winded, but
#     goes like this:
#
#     1. Set up the two Spatial Reference systems.
#     2. Open the original dataset, and get the geotransform
#     3. Calculate bounds of new geotransform by projecting the UL corners
#     4. Calculate the number of pixels with the new projection & spacing
#     5. Create an in-memory raster dataset
#     6. Perform the projection
#     """
#
#
#     pfrom = osr.SpatialReference()
#     pfrom.ImportFromProj4(proj_from)
#     pto = osr.SpatialReference()
#     pto.ImportFromProj4(proj_to)
#     tx = osr.CoordinateTransformation(pfrom, pto)
#
#     # Up to here, all  the projection have been defined, as well as a
#     # transformation from the from to the  to :)
#     # We now open the dataset
#     g = dataset
#     src_b = dataset.GetRasterBand(band or 1)
#
#     # Get the Geotransform vector
#     geo_t = g.GetGeoTransform()
#     x_size = g.RasterXSize  # Raster xsize
#     y_size = g.RasterYSize  # Raster ysize
#     # Work out the boundaries of the new dataset in the target projection
#     (ulx, uly, ulz) = tx.TransformPoint(geo_t[0], geo_t[3])
#     (lrx, lry, lrz) = tx.TransformPoint(geo_t[0] + geo_t[1] * x_size, geo_t[3] + geo_t[5] * y_size)
#
#     deltax = lrx-ulx
#     deltay = lry-uly #very importtant
#     out_xres = deltax/x_size
#     out_yres = deltay/y_size
#
#
#
#     # Now, we create an in-memory raster
#     mem_drv = gdal.GetDriverByName('MEM')
#     # The size of the raster is given the new projection and pixel spacing
#     # Using the values we calculated above. Also, setting it to store one band
#     # and to use Float32 data type.
#     dest = mem_drv.Create('', int(deltax/out_xres), int(deltay/out_yres), 1, src_b.DataType)
#     # Calculate the new geotransform
#     new_geo = (ulx, out_xres, geo_t[2], uly, geo_t[4], out_yres)
#     # Set the geotransform
#     dest.SetGeoTransform(new_geo)
#     dest.SetProjection(pto.ExportToWkt())
#     # Perform the projection/resampling
#
#     gdal.ReprojectImage(g, dest, pfrom.ExportToWkt(), pto.ExportToWkt(),  gdal.GRA_Bilinear)
#     b = dest.GetRasterBand(1)
#     b.SetNoDataValue(0)
#
#
#     return dest
#
#
#
#





#@timeit
def standardize(src_blob_path=None, dst_prj=4326, band=None,
                clip_xmin=-180, clip_xmax=180, clip_ymin=-35, clip_ymax=35, clip_ds=None,
                alternative_path=None, format='GTiff',
                no_cpus=NCPUS, multithread=True,

                ):




    dst_blob_name = os.path.split(src_blob_path)[-1]

    if not 'tif' in dst_blob_name:
        dst_blob_name = f'{os.path.splitext(dst_blob_name)[0]}.tif'

    just_name, ext = os.path.splitext(dst_blob_name)
    dst_blob_name = f'{just_name}_stdz{ext}'

    if alternative_path:
        assert os.path.exists(alternative_path), f'intermediary_folder={alternative_path} does not exist'
        dst_path = os.path.join(alternative_path, dst_blob_name)
        if os.path.exists(dst_path):

            return gdal.OpenEx(dst_path)


    dst_path = f'/vsimem/{dst_blob_name}'

    resampling = gdal.GRA_NearestNeighbour


    # Open source dataset
    src_ds = gdal.Open(src_blob_path)
    dst_bands = band or src_ds.RasterCount
    src_srs = src_ds.GetSpatialRef()


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
    proj_are_equal = bool(src_srs.IsSame(dst_srs))

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
        src = gdal.Translate(vrt_path, src_ds, bandList=band_list,)


        res = gdal.Warp(dst_path,src, options=options)
    else:

        #gdal_translate -r nearest -a_srs EPSG:4326 -projwin_srs '+proj=longlat +datum=WGS84 +pm=0 +over' -projwin -180 30 180 -30 -co "TILED=YES" -co "COMPRESS=LZW" -b 1  GDP_PPP_30arcsec_v3_fixed.tif GDP_PPP_30arcsec_v3.tif

        ce = clip_xmin, clip_ymax, clip_xmax, clip_ymin
        logger.info(f'Clipping {src_blob_path} using {ce}')
        options = gdal.TranslateOptions(format=format,
                                        creationOptions=['COMPRESS=LZW', "TILED=YES"],
                                        projWin=ce,
                                        #projWinSRS='+proj=longlat +datum=WGS84 +pm=0 +over',
                                        outputSRS=dst_srs,
                                        bandList=band_list,
                                        resampleAlg=resampling)
        res = gdal.Translate(dst_path,src_ds, options=options)

    if alternative_path:
        assert os.path.exists(alternative_path), f'alternative_path={alternative_path} does not exist'
        dst_path = os.path.join(alternative_path, dst_blob_name)
        return gdal.Translate(dst_path,res)

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









def run_pipeline(vector_layers_csv_blob=None, raster_layers_csv_blob=None, stat_func_name='mean', sas_url=None, alternative_path=None):


    csv_raster_rows = fetch_raster_rows(raster_layers_src_blob=raster_layers_csv_blob, sas_url=sas_url)

    with ContainerClient.from_container_url(container_url=sas_url) as c:
            vector_cfg_stream = c.download_blob(vector_layers_csv_blob)
            with io.BytesIO() as vstr:
                vector_cfg_stream.readinto(vstr)
                vstr.seek(0)
                vlines = (line.decode('utf-8') for line in vstr.readlines())
                vreader = csv.DictReader(vlines)
                #vfieldnames = vreader.fieldnames
                for csv_vector_rows in vreader:

                    vid = csv_vector_rows['vector_id']
                    if '\\' in csv_vector_rows['file_name']:
                        vfile_name = csv_vector_rows['file_name'].replace('\\', '/')
                    else:
                        vfile_name = csv_vector_rows['file_name']
                    if '\\' in csv_vector_rows['path']:
                        vpath = csv_vector_rows['path'].replace('\\', '/')
                    else:
                        vpath = csv_vector_rows['path']

                    #src_vector_blob_path = os.path.join('/vsiaz/sids/rawdata', vpath, vfile_name)
                    src_vector_blob_path = os.path.join('rawdata', vpath.replace('Shapefile', 'Shapefiles'), vfile_name)

                    vect_ds = fetch_az_shapefile(blob_path=src_vector_blob_path, sas_url=sas_url)
                    for rrow in csv_raster_rows:
                        rid = rrow['attribute_id']
                        if '\\' in rrow['file_name']:
                            rfile_name = rrow['file_name'].replace('\\','/')
                        else:
                            rfile_name = rrow['file_name']
                        if '1a1' in rfile_name:continue
                        if '\\' in rrow['path']:
                            rpath = rrow['path'].replace('\\', '/')
                        else:
                            rpath = rrow['path']
                        #print(rpath)
                        src_raster_blob_path = os.path.join('/vsiaz/sids/', rpath, rfile_name)
                        print(src_raster_blob_path)
                        try:
                            pass

                            # res = zonal_statistics(base_raster_path_band=(src_raster_blob_path, 1), aggregate_vector_path=src_vect)
                            #
                            # print(res)

                            break
                        except Exception as e:
                            logger.info(e)

                            continue


                    break




#
# import csv
# import requests
# import httpio
# from data_pipeline.azblob import HandyContainerClient
# import os
# import io


# id = attribute_raw_file.split('/')[-2]
# gdrive_dowload_url = f'https://drive.google.com/uc?export=download'
# print(f'{gdrive_dowload_url}&id={id}')
# with requests.get(gdrive_dowload_url, params= {'id':id}, stream=True) as resp:
#
#     lines = (line.decode('utf-8') for line in resp.iter_lines())
#     reader = csv.DictReader(lines)
#     for row in reader:
#         print(row)








# def standardize(out_blob_path='3_standardization',  attribute_raw_blob=None, attribute_raw_file=None,  sas_url=None) -> None:
#
#
#
#
#
#     with ContainerClient.from_container_url(container_url=sas_url) as c:
#         stream = c.download_blob(attribute_raw_blob)
#         with io.StringIO() as ostr:
#             with io.BytesIO() as istr:
#                 stream.readinto(istr)
#                 istr.seek(0)
#                 lines = (line.decode('utf-8') for line in istr.readlines())
#                 reader = csv.DictReader(lines)
#                 fieldnames = reader.fieldnames
#
#                 writer = csv.DictWriter(ostr, fieldnames=fieldnames)
#                 writer.writeheader()
#
#                 for row in reader:
#                     print(row)
#                     id = row['attribute_id']
#                     if '\\' in row['path']:
#                         path = row['path'].replace('\\', '/')
#                     else:
#                         path = row['path']
#
#
#                     input_blob_path = os.path.join('/vsiaz/sids/rawdata', path, row['file_name'])
#                     band = int(row['band'])
#
#                     print(input_blob_path)
#
#                     info = gdal.Info(input_blob_path)
#                     print(info)
#                     #
#                     # src_ds = gdal.Open(input_blob_path)
#                     # xres = src_ds.GetGeoTransform()[1]
#                     # if xres <
#                     #
#                     # with rasterio.open(input_blob_path) as ir:
#                     #     print(ir, ir.crs)
#
#
#
#


