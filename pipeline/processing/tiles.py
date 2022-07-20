import shutil
import subprocess
from .utils import logging, cwd, upload_dir

logger = logging.getLogger(__name__)


def export_tiles(r_row, vector_data, r_v_data, _):
    r_id = r_row['id']
    for v_row in vector_data:
        if f"{v_row['id']}_{r_row['id']}" in r_v_data:
            v_id = v_row['id']
            output_path = cwd / f'../outputs/{v_id}_{r_id}'
            output_path.parent.mkdir(parents=True, exist_ok=True)
            vector_path = cwd / f"../tmp/vectors/{v_id}_{r_id}.geojsonl"
            subprocess.run([
                'tippecanoe',
                '--detect-shared-borders',
                '--drop-densest-as-needed',
                '--force',
                f'--layer={v_id}_{r_id}',
                '--maximum-zoom=10',
                '--no-tile-compression',
                '--no-tile-size-limit',
                '--read-parallel',
                '--simplify-only-low-zooms',
                f'--output-to-directory={output_path}',
                vector_path,
            ], stderr=subprocess.DEVNULL)
            upload_dir(output_path)
            shutil.rmtree(output_path, ignore_errors=True)
    logger.info(f'exported {r_id} vector tiles')
