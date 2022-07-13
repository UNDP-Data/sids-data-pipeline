from pathlib import Path
from azure.storage.blob import ContainerClient
from .config import download_path, sas_url
from .utils import cwd, logging

logger = logging.getLogger(__name__)


def list_blobs(data_type):
    container = ContainerClient.from_container_url(sas_url)
    blob_list = container.list_blobs(name_starts_with=f'inputs/{data_type}')
    return list(map(lambda x: {'id': Path(x.name).stem}, blob_list))


def get_rows(rows, data_type, input_ext, tmp_ext):
    for row in rows:
        blob_path = download_path / f"{data_type}/{row['id']}.{input_ext}"
        input_path = cwd / f"../inputs/{data_type}/{row['id']}.{input_ext}"
        tmp_path = cwd / f"../tmp/{data_type}/{row['id']}.{tmp_ext}"
        row['blob_path'] = blob_path
        row['input_path'] = input_path.resolve()
        row['tmp_path'] = tmp_path.resolve()
    return rows


def filter_rasters(raster_data, vector_data):
    r_data = []
    v_r_data = []
    for r_row in raster_data:
        append = False
        for v_row in vector_data:
            tiles = (cwd /
                     f"../outputs/data/{v_row['id']}_{r_row['id']}.mbtiles")
            if not tiles.is_file() or tiles.stat().st_size == 0:
                v_r_data.append(f"{v_row['id']}_{r_row['id']}")
                append = True
        if append:
            r_data.append(r_row)
    return r_data, v_r_data


def get_data():
    vector_data = list_blobs('vectors')
    raster_data = list_blobs('rasters')
    vector_data = get_rows(vector_data, 'vectors', 'gpkg', 'geojsonl')
    raster_data = get_rows(raster_data, 'rasters', 'tif', 'tif')
    raster_data, raster_vector_data = filter_rasters(raster_data, vector_data)
    logger.info('finished')
    return vector_data, raster_data, raster_vector_data
