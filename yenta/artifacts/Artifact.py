from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from yenta.utils.files import file_hash


@dataclass
class Artifact:

    name: str
    location: str
    date_created: datetime
    hash: Optional[str] = None

    @abstractmethod
    def artifact_hash(self):
        raise NotImplementedError


@dataclass
class FileArtifact(Artifact):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.location: Path = Path(self.location)

    def artifact_hash(self):
        return file_hash(self.location).hexdigest()
