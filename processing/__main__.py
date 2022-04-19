from .cleanup import clean_db, clean_inputs, clean_outputs, clean_tmp
from .data import get_data
from .download import download_data
from .inputs import import_data
from .raster import standardize_rasters
from .stats import generate_stats
from .tiles import export_tiles, upload_tiles
from .utils import logging
from .vector import stats_to_vectors

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info('starting')
    vector_data, raster_data = get_data()
    download_data(vector_data, raster_data)
    standardize_rasters(raster_data)
    clean_inputs('rasters')
    import_data(vector_data, raster_data)
    clean_inputs()
    clean_tmp('rasters')
    generate_stats(vector_data, raster_data)
    stats_to_vectors(vector_data)
    clean_db(vector_data, raster_data)
    export_tiles(vector_data)
    clean_tmp()
    upload_tiles()
    clean_outputs()
    logger.info('finished')
