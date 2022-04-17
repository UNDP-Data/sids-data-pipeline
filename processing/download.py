from .utils import logging, download_file, multiprocess_single

logger = logging.getLogger(__name__)


def download(row, _):
    blob_path = row['blob_path']
    input_path = row['input_path']
    if input_path.is_file() and input_path.stat().st_size > 0:
        pass
    else:
        input_path.parent.mkdir(parents=True, exist_ok=True)
        download_file(blob_path, input_path)
        if input_path.suffix == '.shp':
            for e in ('.cpg', '.dbf', '.prj', '.shx'):
                download_file(blob_path.with_suffix(e),
                              input_path.with_suffix(e))
        logger.info(f'downloaded {input_path.name}')


def download_data(vector_data, raster_data):
    multiprocess_single(download, vector_data)
    multiprocess_single(download, raster_data)
