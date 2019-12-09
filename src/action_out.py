import json
import sys
from pathlib import Path

from .resource_boto_client import ResourceBotoClient
from .utils import eprint


def action_out(src_path, in_stream):
    input = json.load(in_stream)
    client = ResourceBotoClient(input)

    object_paths = list(filter(Path.is_file, Path(src_path).glob(client.upload_glob)))
    object_key = 'None'
    for object_path in object_paths:
        object_file = str(object_path)
        object_key = str(object_path)[len(src_path)+1:]
        client.upload_file(
            source=object_file,
            key=object_key)
    eprint('Uploaded objects: ' + str(object_paths))
    # This is a garbage value
    return {'version': {'key': object_key}}


def main():  # pragma: no cover
    src_path = sys.argv[1]
    print(json.dumps(action_out(src_path, sys.stdin)))


if __name__ == '__main__':  # pragma: no cover
    main()
