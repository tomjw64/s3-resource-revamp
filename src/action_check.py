import json
import sys

from .resource_boto_client import ResourceBotoClient
from .utils import eprint


def action_check(in_stream):
    input = json.load(in_stream)
    client = ResourceBotoClient(input)
    object_keys = client.list_filtered_objects()
    eprint('Versions found: ' + str(object_keys))
    return object_keys


def main():  # pragma: no cover
    output = json.dumps(action_check(sys.stdin))
    eprint('Output: ' + output)
    print(output)


if __name__ == '__main__':  # pragma: no cover
    main()
