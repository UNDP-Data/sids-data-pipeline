import subprocess
from .config import epsg, ymin, xmin, ymax, xmax
from .utils import logging

logger = logging.getLogger(__name__)
common_options = ['-q', '--config', 'GDAL_NUM_THREADS', 'ALL_CPUS',
                  '-co', 'COMPRESS=ZSTD', '-co', 'TILED=YES',
                  '-co', 'BLOCKXSIZE=128', '-co', 'BLOCKYSIZE=128']


def standardize_raster(row, cur):
    output_path = row['output_path']
    input_path = row['input_path']
    gdalsrsinfo = ['gdalsrsinfo', '-e', input_path]
    result = subprocess.run(gdalsrsinfo, capture_output=True)
    epsg_num = epsg
    if len(result.stdout) > 0:
        epsg_str = str(result.stdout.splitlines()[1])
        epsg_num = ''.join(filter(str.isdigit, epsg_str))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if epsg_num != epsg:
        transform = subprocess.run([
            'gdalwarp',
            *common_options,
            '-multi',
            '-t_srs', f'EPSG:{epsg}',
            '-te', xmin, ymax, xmax, ymin,
            input_path, output_path,
        ])
    else:
        transform = subprocess.run([
            'gdal_translate',
            *common_options,
            '-b', row['band'],
            '-a_srs', f'EPSG:{epsg}',
            '-projwin', xmin, ymax, xmax, ymin,
            input_path, output_path,
        ])
    if transform.returncode == 0:
        cur.execute("INSERT into rasters values (:id)", {'id': row['id']})
        logger.info(row['id'])
