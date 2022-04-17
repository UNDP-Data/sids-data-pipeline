import subprocess
from .config import epsg, ymin, xmin, ymax, xmax
from .utils import logging

logger = logging.getLogger(__name__)
common_options = ['-q', '--config', 'GDAL_NUM_THREADS', 'ALL_CPUS',
                  '-co', 'TILED=YES',
                  '-co', 'BLOCKXSIZE=128', '-co', 'BLOCKYSIZE=128',
                  '-co', 'COMPRESS=DEFLATE', '-co', 'ZLEVEL=9']


def standardize_raster(row, _):
    tmp_path = row['tmp_path']
    input_path = row['input_path']
    if not tmp_path.is_file() or tmp_path.stat().st_size == 0:
        gdalsrsinfo = ['gdalsrsinfo', '-e', input_path]
        result = subprocess.run(gdalsrsinfo, capture_output=True)
        epsg_num = epsg
        if len(result.stdout) > 0:
            epsg_str = str(result.stdout.splitlines()[1])
            epsg_num = ''.join(filter(str.isdigit, epsg_str))
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        if epsg_num != epsg:
            subprocess.run([
                'gdalwarp',
                *common_options,
                '-multi',
                '-t_srs', epsg,
                '-te', xmin, ymax, xmax, ymin,
                input_path, tmp_path,
            ])
            logger.info(f"projected {row['id']} raster to EPSG:{epsg}")
        else:
            subprocess.run([
                'gdal_translate',
                *common_options,
                '-a_srs', f'EPSG:{epsg}',
                '-projwin', xmin, ymax, xmax, ymin,
                input_path, tmp_path,
            ])
            bbox = f'[{xmin}, {ymax}, {xmax}, {ymin}]'
            logger.info(f"clipped {row['id']} raster to {bbox}")


def standardize_rasters(raster_data):
    for row in raster_data:
        standardize_raster(row, None)
