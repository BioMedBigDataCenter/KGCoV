from tqdm.autonotebook import tqdm as _tqdm
from tqdm.notebook import tqdm as tqdm_notebook
import os


def tqdm(*args, **kwargs):
    if os.environ.get('VSCODE_PID'):
        # Running in VSCode jupyter
        return tqdm_notebook(*args, **kwargs)
    return _tqdm(*args, **kwargs)
