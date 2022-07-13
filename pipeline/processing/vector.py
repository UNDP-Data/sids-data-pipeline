import subprocess
from psycopg2.sql import SQL, Identifier
from .utils import DATABASE, cwd, logging

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


def stats_to_vector(r_row, vector_data, r_v_data, cur):
    r_id = r_row['id'].lower()
    for v_row in vector_data:
        if f"{v_row['id']}_{r_row['id']}" in r_v_data:
            v_id = v_row['id']
            cur.execute(SQL(query_1).format(
                table_in1=Identifier(v_id),
                table_in2=Identifier(f'{v_id}_{r_id}'),
                view_out=Identifier(f'{v_id}_{r_id}_view'),
            ))
            tmp_path = (cwd /
                        f"../tmp/vectors/{v_row['id']}_{r_row['id']}.geojsonl")
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.unlink(missing_ok=True)
            subprocess.run([
                'ogr2ogr',
                tmp_path,
                f'PG:dbname={DATABASE}', f'{v_id}_{r_id}_view',
            ])
            cur.execute(SQL(query_2).format(
                view_out=Identifier(f'{v_id}_{r_id}_view'),
            ))
    logger.info(f'merged {r_id} raster stats with vector geometry')
