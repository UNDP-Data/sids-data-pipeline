from .utils import cwd, logging, write_json

logger = logging.getLogger(__name__)

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


def generate_config():
    files = (cwd / '../outputs/data').glob('*.mbtiles')
    for file in files:
        config['data'][file.stem] = {'mbtiles': file.name}
    write_json(cwd / f'../outputs/config.json', config)
    logger.info('generated config')
