import os
from pathlib import Path

download_path = Path('inputs')
upload_path = Path('inputs/rasters')

sas_url = os.environ.get('SAS_SIDS_CONTAINER')
azure_container = sas_url.split('?')[0]
azure_sas = sas_url.split('?')[1]

epsg = os.environ.get('EPSG', '4326')
xmin = os.environ.get('CLIP_XMIN', '-180')
xmax = os.environ.get('CLIP_XMAX', '180')
ymin = os.environ.get('CLIP_YMIN', '-35')
ymax = os.environ.get('CLIP_YMAX', '35')
