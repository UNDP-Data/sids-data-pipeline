import subprocess
from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import DATABASE, logging, multiprocess_single

logger = logging.getLogger(__name__)
query_1 = """
    DROP VIEW IF EXISTS {view_out};
    CREATE VIEW {view_out} AS
    SELECT a.geom, b.*
    FROM {table_in1} AS a
    LEFT JOIN {table_in2} AS b
    ON a.fid = b.fid;
"""
query_2 = """
    DROP VIEW IF EXISTS {view_out};
"""


def stats_to_vector(row, _):
    v_id = row['id'].replace('-', '_')
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    cur.execute(SQL(query_1).format(
        table_in1=Identifier(v_id),
        table_in2=Identifier(f'{v_id}_stats'),
        view_out=Identifier(f'{v_id}_view'),
    ))
    row['tmp_path'].parent.mkdir(parents=True, exist_ok=True)
    subprocess.run([
        'ogr2ogr',
        '-makevalid',
        '-overwrite',
        row['tmp_path'],
        f'PG:dbname={DATABASE}', f'{v_id}_view',
    ])
    cur.execute(SQL(query_2).format(
        view_out=Identifier(f'{v_id}_view'),
    ))
    cur.close()
    con.close()
    logger.info(f'merged {v_id} vector geometry with stats')


def stats_to_vectors(vector_data):
    multiprocess_single(stats_to_vector, vector_data)
