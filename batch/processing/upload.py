import csv
import sqlite3
import subprocess
from .config import download_path, upload_path
from .utils import logging, cwd, get_azure_url

logger = logging.getLogger(__name__)


def upload(row, *_):
    url = get_azure_url(upload_path / row['output_path'].name)
    subprocess.run(['azcopy', 'copy', row['output_path'], url],
                   stdout=subprocess.DEVNULL)
    # logger.info(row['id'])


def get_writer():
    csv_path = cwd / '../inputs/rasters.csv'
    csv_path.unlink(missing_ok=True)
    csv_file = open(csv_path, 'w', newline='')
    fieldnames = ['id']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    return csv_file, writer


def get_rows():
    db = cwd / '../inputs/rasters.db'
    con = sqlite3.connect(db)
    cur = con.cursor()
    cur.execute('SELECT * FROM rasters')
    return cur.fetchall()


def upload_csv():
    csv_file, writer = get_writer()
    rows = get_rows()
    for row in rows:
        writer.writerow({'id': row[0]})
    csv_file.close()
    url = get_azure_url(download_path / 'rasters.csv')
    subprocess.run(['azcopy', 'copy', cwd / '../inputs/rasters.csv', url],
                   stdout=subprocess.DEVNULL)
    logger.info(f'rasters.csv')


def clean_output(row, *_):
    row['output_path'].unlink(missing_ok=True)
