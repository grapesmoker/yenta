from hashlib import sha1
from pathlib import Path


def file_hash(path: Path, block_size=65536):
    s = sha1()

    if path.exists():
        with open(path, 'rb') as f:
            stop = False
            while not stop:
                data = f.read(block_size)
                if len(data) > 0:
                    s.update(data)
                else:
                    stop = True

    return s
