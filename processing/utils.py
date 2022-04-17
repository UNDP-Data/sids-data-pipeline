import logging
import subprocess
from multiprocessing import Pool
from pathlib import Path
from .config import azure_container, azure_sas

DATABASE = 'sids_data_pipeline'

cwd = Path(__file__).parent
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def get_azure_url(blob_path):
    return f'{azure_container}/{blob_path}?{azure_sas}'


def download_file(blob_path, input_path):
    url = get_azure_url(blob_path)
    subprocess.run(['azcopy', 'copy', url, input_path],
                   stdout=subprocess.DEVNULL)


def multiprocess_dual(func, vector_data, raster_data):
    results = []
    pool = Pool()
    for v_row in vector_data:
        for r_row in raster_data:
            result = pool.apply_async(func, args=[v_row, r_row])
            results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def multiprocess_single(func, data_1, data_2=None):
    results = []
    pool = Pool()
    for row in data_1:
        result = pool.apply_async(func, args=[row, data_2])
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()
