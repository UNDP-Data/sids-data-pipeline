from psycopg2.sql import SQL, Identifier
from .utils import logging

logger = logging.getLogger(__name__)
query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    WITH x AS (
        SELECT a.fid, b.val
        FROM {table_in_v} AS a
        LEFT JOIN {table_in_r} as b
        ON ST_Intersects(a.geom, b.geom)
    )
    SELECT fid, AVG(val) AS mean
    FROM x
    GROUP BY fid
    ORDER BY fid;
"""


def generate_stats(r_row, vector_data, r_v_data, cur):
    for v_row in vector_data:
        v_id = v_row['id']
        r_id = r_row['id'].lower()
        if f"{v_row['id']}_{r_row['id']}" in r_v_data:
            cur.execute(SQL(query_1).format(
                table_in_v=Identifier(v_id),
                table_in_r=Identifier(r_id),
                table_out=Identifier(f'{v_id}_{r_id}'),
            ))
    logger.info(f'calculated {r_id} stats')
