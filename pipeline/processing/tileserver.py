from .utils import cwd, logging, write_json

logger = logging.getLogger(__name__)


def make_config(vector_data, raster_data, input_path):
    config = {
        'options': {
            'paths': {
                'root': '',
                'fonts': 'fonts',
                'sprites': 'sprites',
                'styles': 'styles',
                'mbtiles': 'data'
            },
            'serveAllStyles': True,
            'serveAllFonts': True
        },
        'data': {}
    }
    for v_row in vector_data:
        for r_row in raster_data:
            name = f"{v_row['id']}_{r_row['id']}"
            config['data'][name] = {'mbtiles': f'{name}.mbtiles'}
    write_json(input_path, config)


def generate_config(vector_data, raster_data):
    output_path = cwd / f'../outputs/config.json'
    make_config(vector_data, raster_data, output_path)
    logger.info('generated config')
