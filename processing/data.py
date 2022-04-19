import pandas as pd
from .config import download_path, raster_ids, vector_ids
from .utils import cwd, logging, download_file

logger = logging.getLogger(__name__)


def format_rows(rows, data_type, input_ext, tmp_ext):
    for row in rows:
        blob_path = download_path / f"{data_type}/{row['id']}.{input_ext}"
        input_path = cwd / f"../inputs/{data_type}/{row['id']}.{input_ext}"
        tmp_path = cwd / f"../tmp/{data_type}/{row['id']}.{tmp_ext}"
        row['blob_path'] = blob_path
        row['input_path'] = input_path.resolve()
        row['tmp_path'] = tmp_path.resolve()
    return rows


def get_rows(data_type, filter_id, input_ext, tmp_ext):
    rows = pd.read_csv(cwd / f'../inputs/{data_type}.csv').to_dict('records')
    all_ids = sorted(set([e['id'] for e in rows]))
    if ''.join(filter_id) != '':
        if not set(filter_id).issubset(all_ids):
            f_ids = ', '.join(filter_id)
            ids = ', '.join(all_ids)
            exc = f'invalid id "{f_ids}", valid values are "{ids}"'
            raise Exception(exc)
        rows = list(filter(lambda x: x['id'] in filter_id, rows))
    if len(rows) == 0:
        logger.warning('no files are going to be processed, exiting now')
        exit()
    return format_rows(rows, data_type, input_ext, tmp_ext)


def download_if_missing(data_type):
    blob_path = download_path / f'{data_type}.csv'
    input_path = cwd / f'../inputs/{data_type}.csv'
    if input_path.is_file() and input_path.stat().st_size > 0:
        pass
    else:
        input_path.parent.mkdir(parents=True, exist_ok=True)
        download_file(blob_path, input_path)
        logger.info(f'downloaded {input_path.name}')


def get_data():
    download_if_missing('vectors')
    download_if_missing('rasters')
    vector_data = get_rows('vectors', vector_ids, 'gpkg', 'geojsonl')
    raster_data = get_rows('rasters', raster_ids, 'tif', 'tif')
    return vector_data, raster_data
