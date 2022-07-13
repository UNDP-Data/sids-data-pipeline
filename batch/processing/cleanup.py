import shutil
from .utils import cwd, logging

logger = logging.getLogger(__name__)


def clean_input(row, *_):
    row['input_path'].unlink(missing_ok=True)


def clean_output(row, *_):
    row['output_path'].unlink(missing_ok=True)


def clean_all(*_):
    shutil.rmtree(cwd / '../inputs', ignore_errors=True)
    shutil.rmtree(cwd / '../outputs', ignore_errors=True)
