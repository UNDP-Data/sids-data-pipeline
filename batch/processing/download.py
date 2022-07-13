from .utils import logging, download_file

logger = logging.getLogger(__name__)


def download_data(row, *_):
    blob_path = row['blob_path']
    input_path = row['input_path']
    input_path.parent.mkdir(parents=True, exist_ok=True)
    download_file(blob_path, input_path)
    # logger.info(row['id'])
