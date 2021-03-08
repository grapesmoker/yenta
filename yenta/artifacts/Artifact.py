from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

from yenta.utils.files import file_hash


@dataclass
class Artifact:

    location: Union[str, Path]
    date_created: str = None
    hash: Optional[str] = None
    meta: dict = None

    def artifact_hash(self):
        raise NotImplementedError

    def __post_init__(self):

        if not self.date_created:
            self.date_created = str(datetime.now())

    def __eq__(self, other):

        return self.location == other.location and self.hash == other.hash


@dataclass
class FileArtifact(Artifact):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._path: Path = Path(self.location)
        if self._path.exists() and not self._path.is_dir():
            self.hash = self.artifact_hash()

    def artifact_hash(self):
        return file_hash(self._path).hexdigest()
