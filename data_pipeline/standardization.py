import logging
from osgeo import osr, gdal

logger = logging.getLogger(__name__)

def reproject_dataset(dataset=None,band=None, proj_from=None, proj_to=None) -> str:
    """
    Reproject a
    :param dataset:
    :param band:
    :param proj_from:
    :param proj_to:
    :return:
    """

    """
    A sample function to reproject and resample a GDAL dataset from within
    Python. The idea here is to reproject from one system to another, as well
    as to change the pixel size. The procedure is slightly long-winded, but
    goes like this:

    1. Set up the two Spatial Reference systems.
    2. Open the original dataset, and get the geotransform
    3. Calculate bounds of new geotransform by projecting the UL corners
    4. Calculate the number of pixels with the new projection & spacing
    5. Create an in-memory raster dataset
    6. Perform the projection
    """


    pfrom = osr.SpatialReference()
    pfrom.ImportFromProj4(proj_from)
    pto = osr.SpatialReference()
    pto.ImportFromProj4(proj_to)
    tx = osr.CoordinateTransformation(pfrom, pto)

    # Up to here, all  the projection have been defined, as well as a
    # transformation from the from to the  to :)
    # We now open the dataset
    g = dataset
    src_b = dataset.GetRasterBand(band or 1)

    # Get the Geotransform vector
    geo_t = g.GetGeoTransform()
    x_size = g.RasterXSize  # Raster xsize
    y_size = g.RasterYSize  # Raster ysize
    # Work out the boundaries of the new dataset in the target projection
    (ulx, uly, ulz) = tx.TransformPoint(geo_t[0], geo_t[3])
    (lrx, lry, lrz) = tx.TransformPoint(geo_t[0] + geo_t[1] * x_size, geo_t[3] + geo_t[5] * y_size)

    deltax = lrx-ulx
    deltay = lry-uly #very importtant
    out_xres = deltax/x_size
    out_yres = deltay/y_size



    # Now, we create an in-memory raster
    mem_drv = gdal.GetDriverByName('MEM')
    # The size of the raster is given the new projection and pixel spacing
    # Using the values we calculated above. Also, setting it to store one band
    # and to use Float32 data type.
    dest = mem_drv.Create('', int(deltax/out_xres), int(deltay/out_yres), 1, src_b.DataType)
    # Calculate the new geotransform
    new_geo = (ulx, out_xres, geo_t[2], uly, geo_t[4], out_yres)
    # Set the geotransform
    dest.SetGeoTransform(new_geo)
    dest.SetProjection(pto.ExportToWkt())
    # Perform the projection/resampling

    gdal.ReprojectImage(g, dest, pfrom.ExportToWkt(), pto.ExportToWkt(),  gdal.GRA_Bilinear)
    b = dest.GetRasterBand(1)
    b.SetNoDataValue(0)


    return dest

import csv
import requests
import httpio

def standardize(dst_folder=None,  attribute_raw_file=None) -> None:
    # read attribute file
    attributes = list()

    id = attribute_raw_file.split('/')[-2]
    gdrive_dowload_url = f'https://drive.google.com/uc?export=download'
    print(f'{gdrive_dowload_url}&id={id}')
    with requests.get(gdrive_dowload_url, params= {'id':id}, stream=True) as resp:

        lines = (line.decode('utf-8') for line in resp.iter_lines())
        reader = csv.DictReader(lines)
        for row in reader:
            print(row)


    # open(attribute_raw_file, 'r', encoding='latin1')
    # header = f_in.readline().strip().split(",")
    # reader = csv.reader(f_in)
    # for row in reader:
    #     attributes.append(row)
    # f_in.close()
    #
    # # output file
    # f_out = open(attribute_update_file, 'w', encoding='latin1', newline='')
    # writer = csv.writer(f_out)
    # writer.writerow(header)