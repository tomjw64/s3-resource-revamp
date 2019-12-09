import boto3
from functools import reduce

from .utils import (eprint,
                    deep_get,
                    parse_semver_array_from_string,
                    key_regexp_matches,
                    key_regexp_max_version,
                    key_regexp_version_not_less_than)


class ResourceBotoClient:
    def __init__(self, input):
        self.init_source_options(input)
        self.init_version(input)
        self.init_params(input)
        self.client = boto3.client(
            service_name=self.service,
            region_name=self.region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            endpoint_url=self.endpoint)

    def init_source_options(self, input):
        self.service = 's3'
        self.region = deep_get(input, 'source', 'service', 'region')
        self.bucket = deep_get(input, 'source', 'service', 'bucket')
        self.endpoint = deep_get(input, 'source', 'service', 'endpoint', default='http://s3.amazonaws.com')

        self.access_key_id = deep_get(input, 'source', 'credentials', 'access_key_id')
        self.secret_access_key = deep_get(input, 'source', 'credentials', 'secret_access_key')

        self.prefix_filter = deep_get(input, 'source', 'filters', 'prefix', default='')
        self.regexp_filter = deep_get(input, 'source', 'filters', 'regexp')
        self.version_filter = deep_get(input, 'source', 'filters', 'version')

    def init_version(self, input):
        self.version_key = deep_get(input, 'version', 'key')

    def init_params(self, input):
        self.action_mode = deep_get(input, 'params', 'mode')
        self.upload_glob = deep_get(input, 'params', 'glob')

    def list_filtered_objects(self):
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prefix_filter)
        except self.client.exceptions.NoSuchBucket:  # pragma: no cover
            eprint('No versions found - no bucket')
            return []

        if not self.regexp_filter:
            eprint('No versions found - no regex')
            return []

        try:
            response_objects = response['Contents']
        except KeyError:
            eprint(f'Cannot read \'Contents\' of {response}')
            eprint('No versions found - cannot read or none found')
            return []

        matching_objects = list(filter(key_regexp_matches(self.regexp_filter), response_objects))

        if self.version_filter == 'every':
            filtered_objects = matching_objects
        elif self.version_filter in [None, 'latest']:
            filtered_objects = reduce(key_regexp_max_version(self.regexp_filter), matching_objects, [])
        else:
            threshold_semver = parse_semver_array_from_string(self.version_filter)
            filtered_objects = list(filter(key_regexp_version_not_less_than(self.regexp_filter,
                                                                            threshold_semver),
                                    matching_objects))

        object_keys = [{'key': obj['Key']} for obj in filtered_objects]
        return object_keys

    def download_file(self, *, key, destination):
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(
            Bucket=self.bucket,
            Key=key,
            Filename=str(destination))

    def upload_file(self, *, source, key):
        self.client.upload_file(
            Filename=source,
            Bucket=self.bucket,
            Key=key)
