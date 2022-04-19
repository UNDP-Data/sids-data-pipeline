import subprocess
from .config import upload_path, dry_run
from .utils import logging, cwd, get_azure_url

logger = logging.getLogger(__name__)


def export_tiles(vector_data):
    for row in vector_data:
        v_id = row['id']
        output_path = cwd / f'../outputs/{v_id}'
        output_path.mkdir(parents=True, exist_ok=True)
        subprocess.run([
            'tippecanoe',
            '--detect-shared-borders',
            '--drop-densest-as-needed',
            '--force',
            f'--layer={v_id}',
            '--maximum-zoom=12',
            '--no-tile-compression',
            '--no-tile-size-limit',
            '--read-parallel',
            '--simplify-only-low-zooms',
            f'--output-to-directory={output_path}',
            row['tmp_path'],
        ], stderr=subprocess.DEVNULL)
        logger.info(f'exported {v_id} vector tiles')


def upload_tiles():
    if not dry_run:
        input_path = cwd / '../outputs/*'
        upload_url = get_azure_url(upload_path)
        subprocess.run([
            'azcopy', 'copy', '--recursive',
            input_path, upload_url,
        ])
        logger.info(f'uploaded all vector tiles')
