import shutil
from psycopg2.sql import SQL, Identifier
from .utils import cwd, logging

logger = logging.getLogger(__name__)
query_1 = """
    DROP TABLE IF EXISTS {table_in1};
    DROP TABLE IF EXISTS {table_in2};
"""
query_2 = """
    DROP TABLE IF EXISTS {table_in1};
"""


def clean_input(row, *_):
    row['input_path'].unlink(missing_ok=True)


def clean_tmp_raster(row, *_):
    row['tmp_path'].unlink(missing_ok=True)


def clean_tmp_vector(r_row, vector_data, _):
    for v_row in vector_data:
        tmp_file = f"{v_row['id']}_{r_row['id']}.geojsonl"
        (cwd / f'../tmp/vectors/{tmp_file}').unlink(missing_ok=True)


def clean_db_raster(r_row, vector_data, cur):
    for v_row in vector_data:
        v_id = v_row['id'].replace('-', '_')
        r_id = r_row['id'].replace('-', '_')
        cur.execute(SQL(query_1).format(
            table_in1=Identifier(r_id),
            table_in2=Identifier(f'{v_id}_{r_id}'),
        ))


def clean_db_vector(row, _, cur):
    v_id = row['id'].replace('-', '_')
    cur.execute(SQL(query_2).format(
        table_in1=Identifier(v_id),
    ))
