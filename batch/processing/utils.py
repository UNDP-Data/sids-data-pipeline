import csv
import logging
import subprocess
import sqlite3
from multiprocessing import Pool
from pathlib import Path
from .config import azure_container, azure_sas

cwd = Path(__file__).parent
db = (cwd / '../inputs/rasters.db').resolve()
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


def init_db():
    db.unlink(missing_ok=True)
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db)
    cur = con.cursor()
    cur.execute('CREATE TABLE rasters (id text)')
    con.commit()
    con.close()


def apply_funcs(funcs, row):
    con = sqlite3.connect(db)
    cur = con.cursor()
    for func in funcs:
        func(row, cur)
    con.commit()
    con.close()


def multiprocess(funcs, data):
    results = []
    pool = Pool()
    for row in data:
        result = pool.apply_async(apply_funcs, args=[funcs, row])
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
