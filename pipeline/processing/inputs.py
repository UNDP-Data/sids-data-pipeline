import subprocess
from psycopg2.sql import SQL, Identifier
from .utils import DATABASE, logging

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    WITH x AS (
        SELECT (ST_PixelAsPolygons(rast)).*
        FROM {table_in}
    )
    SELECT val, geom
    FROM x;
    CREATE INDEX ON {table_out} USING GIST(geom);
"""
query_2 = """
    DROP TABLE IF EXISTS {table_in};
"""


def import_raster(row, _, __, cur):
    subprocess.run(' '.join([
        'raster2pgsql',
        '-d', '-r', '-C', '-I', '-Y',
        '-t', '128x128',
        str(row['input_path']),
        row['id'] + '_rast',
        '|',
        'psql',
        '--quiet',
        '-d', DATABASE,
    ]), shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    r_id = row['id'].lower()
    cur.execute(SQL(query_1).format(
        table_in=Identifier(f'{r_id}_rast'),
        table_out=Identifier(r_id),
    ))
    cur.execute(SQL(query_2).format(
        table_in=Identifier(f'{r_id}_rast'),
    ))
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
        '-lco', 'LAUNDER=NO',
        '-nln', row['id'],
        '-f', 'PostgreSQL', f'PG:dbname={DATABASE}',
        row['input_path'],
    ])
    logger.info(f"imported {row['id']} vector to database")
