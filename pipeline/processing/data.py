from .config import download_path
from .utils import cwd, logging, read_csv, download_file

logger = logging.getLogger(__name__)


def get_rows(data_type, input_ext, tmp_ext):
    rows = read_csv(cwd / f'../inputs/{data_type}.csv')
    for row in rows:
        blob_path = download_path / f"{data_type}/{row['id']}.{input_ext}"
        input_path = cwd / f"../inputs/{data_type}/{row['id']}.{input_ext}"
        tmp_path = cwd / f"../tmp/{data_type}/{row['id']}.{tmp_ext}"
        row['blob_path'] = blob_path
        row['input_path'] = input_path.resolve()
        row['tmp_path'] = tmp_path.resolve()
    return rows


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
    vector_data = get_rows('vectors', 'gpkg', 'geojsonl')
    raster_data = get_rows('rasters', 'tif', 'tif')
    return vector_data, raster_data
