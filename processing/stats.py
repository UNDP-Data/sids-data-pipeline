from psycopg2 import connect
from psycopg2.sql import SQL, Identifier
from .utils import DATABASE, logging

logger = logging.getLogger(__name__)
query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    WITH x AS (
        SELECT
            a.fid,
            ST_Clip(b.rast, a.geom) AS rast
        FROM {table_in_v} AS a
        LEFT JOIN {table_in_r} as b
        ON ST_Intersects(a.geom, b.rast)
    )
    SELECT
        fid,
        (ST_SummaryStatsAgg(rast, 1, true)).mean AS mean,
    FROM x
    GROUP BY fid
    ORDER BY fid;
"""


def data_stats(v_row, r_row):
    v_id = v_row['id'].replace('-', '_')
    r_id = r_row['id'].replace('-', '_')
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    cur.execute(SQL(query_1).format(
        table_in_v=Identifier(v_id),
        table_in_r=Identifier(r_id),
        table_out=Identifier(f'{v_id}_{r_id}'),
    ))
    cur.close()
    con.close()
    logger.info(f'calculated {v_id} vector stats for {r_id} raster')
