from pathlib import Path
from .config import download_path
from .utils import cwd, logging, read_csv, download_file

logger = logging.getLogger(__name__)


def get_rows():
    rows = read_csv(cwd / f'../inputs/batch.csv')
    for row in rows:
        blob_path = Path(row['Path'], row['fileName'])
        ext = Path(row['fileName']).suffix
        input_path = cwd / f"../inputs/{row['layerId']}{ext}"
        output_path = cwd / f"../outputs/{row['layerId']}.tif"
        row['id'] = row['layerId']
        row['blob_path'] = blob_path
        row['input_path'] = input_path.resolve()
        row['output_path'] = output_path.resolve()
    return rows


def download_if_missing():
    blob_path = download_path / f'batch.csv'
    input_path = cwd / f'../inputs/batch.csv'
    if input_path.is_file() and input_path.stat().st_size > 0:
        pass
    else:
        input_path.parent.mkdir(parents=True, exist_ok=True)
        download_file(blob_path, input_path)
        logger.info(f'downloaded {input_path.name}')


def get_data():
    download_if_missing()
    raster_data = get_rows()
    return raster_data
