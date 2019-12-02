import json
import sys
import boto3
import re
from functools import reduce

from .utils import deep_get


def parse_semver_array_from_string(semver_string):
    return [int(x) for x in semver_string.split('.')]


def version_from_response_object(response_object, regexp_filter):
    key = response_object['Key']
    match = re.match(regexp_filter, key)
    groups = match.groups()
    if len(groups) == 0:
        return -1
    elif len(groups) == 1:
        version = parse_semver_array_from_string(groups[0])
    else:
        version_group = match.group('version')
        version = parse_semver_array_from_string(version_group)
    return version


def key_regexp_matches(regexp_filter):
    def partial(response_object):
        key = response_object['Key']
        match = re.match(regexp_filter, key)
        return match is not None
    return partial


def key_regexp_max_version(regexp_filter):
    def partial(last_max, response_object):
        if len(last_max) == 0:
            return [response_object]

        this_object_version = version_from_response_object(response_object, regexp_filter)
        last_max_version = version_from_response_object(last_max[0], regexp_filter)

        if this_object_version == last_max_version:
            this_object_version = response_object['LastModified']
            last_max_version = last_max[0]['LastModified']

        if this_object_version > last_max_version:
            return [response_object]
        return last_max
    return partial


def key_regexp_version_not_less_than(regexp_filter, threshold_semver):
    def partial(response_object):
        this_object_version = version_from_response_object(response_object, regexp_filter)
        return not this_object_version < threshold_semver
    return partial


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
        endpoint_url=endpoint
    )

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix_filter
    )

    if response == '' or not regexp_filter:
        return []

    response_objects = json.loads(response)['Contents']
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


def main():
    print(json.dumps(action_check(sys.stdin)))  # pragma: no cover


if __name__ == '__main__':
    main()  # pragma: no cover
