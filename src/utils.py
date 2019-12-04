import sys
import re
from functools import reduce


def deep_get(initial_dict, *path, default=None):
    def inner_dict_or_none(acc, step):
        try:
            return acc.get(step)
        except AttributeError:
            return None
    return reduce(inner_dict_or_none, path, initial_dict) or default


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


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
