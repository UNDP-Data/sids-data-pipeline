import os
from configparser import ConfigParser
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
cwd = Path(__file__).parent
cfg = ConfigParser()
cfg.read(cwd / '../config.ini')
config = cfg['default']

download_path = Path(config.get('download_path', 'inputs'))
upload_path = Path(config.get('upload_path', 'tiles'))

vector_ids = config.get('vector_ids').split(',')
raster_ids = config.get('raster_ids').split(',')

keep_files = config.getboolean('keep_files')
dry_run = config.getboolean('dry_run')

sas_url = config.get('sas_url') or os.environ.get('SAS_SIDS_CONTAINER')
azure_container = sas_url.split('?')[0]
azure_sas = sas_url.split('?')[1]

epsg = os.environ.get('EPSG', '4326')
xmin = os.environ.get('CLIP_XMIN', '-180')
xmax = os.environ.get('CLIP_XMAX', '180')
ymin = os.environ.get('CLIP_YMIN', '-35')
ymax = os.environ.get('CLIP_YMAX', '35')
