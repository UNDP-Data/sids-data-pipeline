import shutil
from psycopg2.sql import SQL, Identifier
from .utils import cwd, logging

logger = logging.getLogger(__name__)
query_1 = """
    DROP TABLE IF EXISTS {table_in};
"""


def clean_download(row, *_):
    row['input_path'].unlink(missing_ok=True)


def clean_tmp_raster(row, *_):
    row['tmp_path'].unlink(missing_ok=True)


def clean_tmp_vector(r_row, vector_data, *_):
    for v_row in vector_data:
        tmp_file = f"{v_row['id']}_{r_row['id']}.geojsonl"
        (cwd / f'../tmp/vectors/{tmp_file}').unlink(missing_ok=True)


def clean_db_input(row, _, __, cur):
    r_id = row['id'].lower()
    cur.execute(SQL(query_1).format(
        table_in=Identifier(r_id),
    ))


def clean_db_raster(r_row, vector_data, _, cur):
    for v_row in vector_data:
        v_id = v_row['id']
        r_id = r_row['id'].lower()
        cur.execute(SQL(query_1).format(
            table_in=Identifier(f'{v_id}_{r_id}'),
        ))


def clean_db_vector(row, _, __, cur):
    v_id = row['id']
    cur.execute(SQL(query_1).format(
        table_in=Identifier(v_id),
    ))


def clean_all(*_):
    shutil.rmtree(cwd / '../inputs', ignore_errors=True)
    shutil.rmtree(cwd / '../tmp', ignore_errors=True)
    shutil.rmtree(cwd / '../outputs', ignore_errors=True)
