import json
import logging
import subprocess
from multiprocessing import Pool
from pathlib import Path
from psycopg2 import connect
from .config import azure_container, azure_sas, sas_data_url

DATABASE = 'sids_data_pipeline'

cwd = Path(__file__).parent
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def write_json(output_path, data):
    with open(output_path, 'w') as f:
        json.dump(data, f, separators=(',', ':'))


def get_azure_url(blob_path):
    return f'{azure_container}/{blob_path}?{azure_sas}'


def download_file(blob_path, input_path):
    url = get_azure_url(blob_path)
    subprocess.run(['azcopy', 'copy', url, input_path],
                   stdout=subprocess.DEVNULL)


def upload_dir(input_path):
    subprocess.run(['azcopy', 'copy', '--recursive=true', input_path, sas_data_url],
                   stdout=subprocess.DEVNULL)


def apply_funcs(funcs, row, data_2, data_3):
    con = connect(database=DATABASE)
    con.set_session(autocommit=True)
    cur = con.cursor()
    for func in funcs:
        func(row, data_2, data_3, cur)
    cur.close()
    con.close()


def multiprocess(funcs, data_1, data_2=None, data_3=None):
    results = []
    pool = Pool()
    for row in data_1:
        args = [funcs, row, data_2, data_3]
        result = pool.apply_async(apply_funcs, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
