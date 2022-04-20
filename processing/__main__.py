from .cleanup import (clean_input, clean_tmp_raster, clean_tmp_vector,
                      clean_output, clean_db_vector, clean_db_raster,
                      clean_all)
from .data import get_data
from .download import download_data
from .inputs import import_raster, import_vector
from .raster import standardize_raster
from .stats import generate_stats
from .tiles import export_tiles, upload_tiles
from .utils import logging, multiprocess
from .vector import stats_to_vector

logger = logging.getLogger(__name__)

vector_funcs = [download_data, import_vector, clean_input]
raster_funcs = [download_data, standardize_raster, clean_input, import_raster,
                clean_tmp_raster, generate_stats, stats_to_vector,
                clean_db_raster, export_tiles, clean_tmp_vector, upload_tiles,
                clean_output]
vector_cleanup = [clean_db_vector, clean_all]

if __name__ == '__main__':
    logger.info('starting')
    vector_data, raster_data = get_data()
    multiprocess(vector_funcs, vector_data)
    multiprocess(raster_funcs, raster_data, vector_data)
    multiprocess(vector_cleanup, vector_data)
    logger.info('finished')
