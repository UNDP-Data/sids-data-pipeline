from .cleanup import clean_input, clean_output, clean_all
from .data import get_data
from .download import download_data
from .raster import standardize_raster
from .upload import upload, upload_csv
from .utils import logging, multiprocess, init_db

logger = logging.getLogger(__name__)

raster_funcs = [download_data, standardize_raster,
                clean_input, upload, clean_output]

if __name__ == '__main__':
    logger.info('starting')
    init_db()
    raster_data = get_data()
    multiprocess(raster_funcs, raster_data)
    upload_csv()
    clean_all()
    logger.info('finished')
