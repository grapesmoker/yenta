import string

from datetime import datetime
from pathlib import Path
from random import choices

from yenta.artifacts.Artifact import Artifact, FileArtifact


def test_artifact_equality():

    now = datetime.now()

    art1 = Artifact(name='bar', location='foo', date_created=now)
    art2 = Artifact(name='bar', location='foo', date_created=now)

    assert (art1 == art2)


def test_file_artifact_equality():

    now = datetime.now()

    art1 = FileArtifact(name='bar', location=Path('foo'), date_created=now)
    art2 = FileArtifact(name='bar', location=Path('foo'), date_created=now)

    assert (art1 == art2)


def test_file_artifact_hash():

    now = datetime.now()

    data = 'some nice data'

    output_file = Path('tests').resolve() / 'tmp' / 'artifact.test'

    with open(output_file, 'w') as f:
        f.write(data)

    art = FileArtifact(name='foo', location=output_file, date_created=now)
    art.hash = art.artifact_hash()

    assert(art.hash == '6a52cbb539857eb8c7353cadda0054996dea6de8')

    output_file.unlink()

