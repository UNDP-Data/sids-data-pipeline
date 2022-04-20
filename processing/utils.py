import csv
import logging
import subprocess
from multiprocessing import Pool
from pathlib import Path
from psycopg2 import connect
from .config import azure_container, azure_sas

DATABASE = 'sids_data_pipeline'

cwd = Path(__file__).parent
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def read_csv(input_path):
    with open(input_path, newline='') as f:
        return list(csv.DictReader(f))


def get_azure_url(blob_path):
    return f'{azure_container}/{blob_path}?{azure_sas}'


def download_file(blob_path, input_path):
    url = get_azure_url(blob_path)
    subprocess.run(['azcopy', 'copy', url, input_path],
                   stdout=subprocess.DEVNULL)


def apply_funcs(funcs, row, data_2):
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    for func in funcs:
        func(row, data_2, cur)
    cur.close()
    con.close()


def multiprocess(funcs, data_1, data_2=None):
    results = []
    pool = Pool()
    for row in data_1:
        result = pool.apply_async(apply_funcs, args=[funcs, row, data_2])
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
