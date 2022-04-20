import subprocess
from .utils import DATABASE, logging

logger = logging.getLogger(__name__)


def import_raster(row, *_):
    subprocess.run(' '.join([
        'raster2pgsql',
        '-d', '-r', '-C', '-I', '-Y',
        '-t', '128x128',
        str(row['tmp_path']),
        row['id'],
        '|',
        'psql',
        '--quiet',
        '-d', DATABASE,
    ]), shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    logger.info(f"imported {row['id']} raster to database")


def import_vector(row, *_):
    subprocess.run([
        'ogr2ogr',
        '-makevalid',
        '-overwrite',
        '-dim', 'XY',
        '-t_srs', 'EPSG:4326',
        '-nlt', 'PROMOTE_TO_MULTI',
        '-lco', 'FID=fid',
        '-lco', 'GEOMETRY_NAME=geom',
        '-lco', 'OVERWRITE=YES',
        '-nln', row['id'],
        '-f', 'PostgreSQL', f'PG:dbname={DATABASE}',
        row['input_path'],
    ])
    logger.info(f"imported {row['id']} vector to database")
