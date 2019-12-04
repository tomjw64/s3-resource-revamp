import json
import sys
import boto3
from functools import reduce

from .utils import (eprint,
                    deep_get,
                    parse_semver_array_from_string,
                    key_regexp_matches,
                    key_regexp_max_version,
                    key_regexp_version_not_less_than)


def action_check(in_stream):
    input = json.load(in_stream)

    service = 's3'
    region = deep_get(input, 'source', 'service', 'region')
    bucket = deep_get(input, 'source', 'service', 'bucket')
    endpoint = deep_get(input, 'source', 'service', 'endpoint', default='http://s3.amazonaws.com')

    access_key_id = deep_get(input, 'source', 'credentials', 'access_key_id')
    secret_access_key = deep_get(input, 'source', 'credentials', 'secret_access_key')

    prefix_filter = deep_get(input, 'source', 'filters', 'prefix', default='')
    regexp_filter = deep_get(input, 'source', 'filters', 'regexp')
    version_filter = deep_get(input, 'source', 'filters', 'version')

    s3_client = boto3.client(
        service_name=service,
        region_name=region,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        endpoint_url=endpoint)

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix_filter)

    if response == {} or not regexp_filter:
        eprint('No versions found')
        return []

    response_objects = response['Contents']
    matching_objects = list(filter(key_regexp_matches(regexp_filter), response_objects))

    if version_filter == 'every':
        new_versions = matching_objects
    elif version_filter is None or version_filter == 'latest':
        new_versions = reduce(key_regexp_max_version(regexp_filter), matching_objects, [])
    else:
        threshold_semver = parse_semver_array_from_string(version_filter)
        new_versions = list(filter(key_regexp_version_not_less_than(regexp_filter, threshold_semver),
                                   matching_objects))

    return new_versions


def main():  # pragma: no cover
    print(json.dumps(action_check(sys.stdin)))


if __name__ == '__main__':  # pragma: no cover
    main()
