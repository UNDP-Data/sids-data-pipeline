import subprocess
from .config import epsg, ymin, xmin, ymax, xmax
from .utils import logging

logger = logging.getLogger(__name__)
common_options = ['-q', '--config', 'GDAL_NUM_THREADS', 'ALL_CPUS',
                  '-co', 'COMPRESS=ZSTD', '-co', 'PREDICTOR=2',
                  '-co', 'TILED=YES',
                  '-co', 'BLOCKXSIZE=128', '-co', 'BLOCKYSIZE=128']


def standardize_raster(row, *_):
    tmp_path = row['tmp_path']
    input_path = row['input_path']
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
    else:
        subprocess.run([
            'gdal_translate',
            *common_options,
            '-a_srs', f'EPSG:{epsg}',
            '-projwin', xmin, ymax, xmax, ymin,
            input_path, tmp_path,
        ])
    logger.info(f"standardized {row['id']} raster")
