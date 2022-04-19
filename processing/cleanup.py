import shutil
from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .config import keep_files
from .utils import DATABASE, cwd, logging

logger = logging.getLogger(__name__)
query_1 = """
    DROP TABLE IF EXISTS {table_in1};
    DROP TABLE IF EXISTS {table_in2};
    DROP TABLE IF EXISTS {table_in3};
    DROP TABLE IF EXISTS {table_in4};
"""


def clean_path(path, sub_path):
    if sub_path is not None:
        shutil.rmtree(cwd / f'../{path}/{sub_path}')
    else:
        shutil.rmtree(cwd / f'../{path}')


def clean_inputs(sub_path=None):
    if not keep_files:
        clean_path('inputs', sub_path)
        logger.info('cleaned inputs/ dir')


def clean_db(vector_data, raster_data):
    if not keep_files:
        con = connect(database=DATABASE)
        con.set_session(autocommit=True)
        cur = con.cursor()
        for v_row in vector_data:
            for r_row in raster_data:
                v_id = v_row['id'].replace('-', '_')
                r_id = r_row['id'].replace('-', '_')
                cur.execute(SQL(query_1).format(
                    table_in1=Identifier(v_id),
                    table_in2=Identifier(r_id),
                    table_in3=Identifier(f'{v_id}_{r_id}'),
                    table_in4=Identifier(f'{v_id}_stats'),
                ))
        cur.close()
        con.close()
        logger.info('cleaned db')


def clean_tmp(sub_path=None):
    if not keep_files:
        clean_path('tmp', sub_path)
        logger.info('cleaned tmp/ dir')


def clean_outputs(sub_path=None):
    if not keep_files:
        clean_path('outputs', sub_path)
        logger.info('cleaned outputs/ dir')
