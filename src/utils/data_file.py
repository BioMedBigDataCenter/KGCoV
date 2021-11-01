from datetime import date
from shutil import rmtree
import mmap
from os import makedirs
from os.path import join, isdir, isfile, dirname, abspath


def filepath(*path):
    return abspath(join(*path))


def clean_dir(*path):
    path = filepath(*path)
    rmtree(path, ignore_errors=True)
    makedirs(path, exist_ok=True)
    return path


def ensure_dir(*path):
    path = filepath(*path)
    # dir_ = path if isdir(path) else dirname(path)
    makedirs(path, exist_ok=True)
    return path


def file_suffix(filename, suffix):
    try:
        base, ext = filename.rsplit('.', 1)
        ext = '.' + ext
    except ValueError:
        base, ext = filename, ''
    return f'{base}_{suffix}{ext}'


def date_suffix(filename, fixed_to=None):
    today = fixed_to or date.today().strftime('%m%d')
    return file_suffix(filename, suffix=today)


def sample_suffix(filename):
    return file_suffix(filename, suffix='sample')


def line_count(filename):
    # count '\n' number
    f = open(filename, "r+")
    buf = mmap.mmap(f.fileno(), 0)
    lines = 0
    readline = buf.readline
    while readline():
        lines += 1
    return lines


if __name__ == "__main__":
    # print(date_suffix('../test/file.name.ext'))
    print(line_count(__file__))
