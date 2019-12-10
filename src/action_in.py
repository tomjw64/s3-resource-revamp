import json
import sys
from pathlib import Path

from .resource_boto_client import ResourceBotoClient
from .utils import eprint


def action_in(dest_path, in_stream):
    input = json.load(in_stream)
    client = ResourceBotoClient(input)
    action_mode = client.action_mode
    check_version = client.version_key

    eprint(f'Check version: {check_version}')
    eprint(f'Mode: {action_mode}')

    def download_check_version():
        destination = Path(dest_path) / check_version
        eprint(f'Downloading object - key: {check_version}, dest: {destination}')
        client.download_file(
            key=check_version,
            destination=destination)
        eprint('Object downloaded: ' + check_version)
        return {'version': {'key': check_version}}

    def sync_filtered():
        object_key_dicts = client.list_filtered_objects()
        object_key = None
        for object_key_dict in object_key_dicts:
            object_key = object_key_dict['key']
            destination = Path(dest_path) / object_key
            if not destination.exists():
                eprint(f'Downloading object - key: {object_key}, dest: {destination}')
                client.download_file(
                    key=object_key,
                    destination=destination)

        # This is a garbage value
        return {'version': {'key': object_key}} if object_key is not None else {}

    return {
        'all': sync_filtered,
        'single': download_check_version,
        None: lambda: {}
    }[action_mode]()


def main():  # pragma: no cover
    dest_path = sys.argv[1]
    print(json.dumps(action_in(dest_path, sys.stdin)))


if __name__ == '__main__':  # pragma: no cover
    main()
