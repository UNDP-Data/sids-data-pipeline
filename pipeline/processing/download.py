from .utils import logging, download_file

logger = logging.getLogger(__name__)


def download_data(row, *_):
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
