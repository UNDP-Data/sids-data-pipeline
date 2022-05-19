import subprocess
from .utils import logging, cwd

logger = logging.getLogger(__name__)


def export_tiles(r_row, vector_data, _):
    r_id = r_row['id']
    for v_row in vector_data:
        v_id = v_row['id']
        output_path = cwd / f'../outputs/data/{v_id}_{r_id}.mbtiles'
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = cwd / f"../tmp/vectors/{v_id}_{r_id}.geojsonl"
        subprocess.run([
            'tippecanoe',
            '--detect-shared-borders',
            '--drop-densest-as-needed',
            '--force',
            f'--layer={v_id}_{r_id}',
            '--maximum-zoom=12',
            '--no-tile-size-limit',
            '--read-parallel',
            '--simplify-only-low-zooms',
            f'--output={output_path}',
            tmp_path,
        ], stderr=subprocess.DEVNULL)
    logger.info(f'exported {r_id} vector tiles')
