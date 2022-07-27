from .cleanup import (clean_download, clean_db_input, clean_tmp_vector,
                      clean_db_vector, clean_db_raster, clean_all)
from .data import get_data
from .download import download_data
from .inputs import import_raster, import_vector
from .stats import generate_stats
from .tiles import export_tiles
from .utils import logging, multiprocess
from .vector import stats_to_vector

logger = logging.getLogger(__name__)

vector_funcs = [download_data, import_vector, clean_download]
raster_funcs = [download_data, import_raster, clean_download,
                generate_stats, clean_db_input, stats_to_vector, clean_db_raster,
                export_tiles, clean_tmp_vector]
vector_cleanup = [clean_db_vector]

if __name__ == '__main__':
    logger.info('starting')
    vector_data, raster_data, raster_vector_data = get_data()
    multiprocess(vector_funcs, vector_data)
    multiprocess(raster_funcs, raster_data, vector_data, raster_vector_data)
    multiprocess(vector_cleanup, vector_data)
    clean_all()
    logger.info('finished')
