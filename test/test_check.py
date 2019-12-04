import io
import json
from pathlib import Path

import pytest

from src import action_check


DATA_DIR = Path(__file__).resolve().parent / 'data'


def read_json_file(data_file):
    with open(DATA_DIR / data_file) as f:
        return f.read()


def make_stream(json_compat_dict):
    stream = io.StringIO()
    json.dump(json_compat_dict, stream)
    stream.seek(0)
    return stream


def mock_s3_client_list_objects_responder(mocker, response):
    mock_client = mocker.MagicMock()
    mock_client.list_objects_v2 = mocker.MagicMock(return_value=response)
    return mock_client


class TestCheck:
    def test_check_empty(self, mocker):
        response = read_json_file('list-objects-v2-empty.json')
        mock_client = mock_s3_client_list_objects_responder(mocker, response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {}
        result = action_check.action_check(make_stream(input))
        assert result == []

    def test_check_minio_exact_match_latest(self, mocker):
        response = read_json_file('list-objects-v2-minio.json')
        mock_client = mock_s3_client_list_objects_responder(mocker, response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'source': {
                'filters': {
                    'regexp': 'file.txt',
                    'version': 'latest'
                }
            }
        }
        result = action_check.action_check(make_stream(input))
        assert result[0]['Key'] == 'file.txt'

    def test_check_minio_bad_version(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-minio.json')
        mock_client = mock_s3_client_list_objects_responder(mocker, response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'source': {
                'filters': {
                    'regexp': 'file.txt',
                    'version': 'asdf'
                }
            }
        }
        with pytest.raises(ValueError):
            action_check.action_check(make_stream(input))

    def test_check_minio_multiple_match_latest(self, mocker):
        response = read_json_file('list-objects-v2-minio-multiple-match.json')
        mock_client = mock_s3_client_list_objects_responder(mocker, response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'source': {
                'filters': {
                    'regexp': r'file.te?xt',
                    'version': 'latest'
                }
            }
        }
        result = action_check.action_check(make_stream(input))
        assert len(result) == 1
        assert result[0]['Key'] == 'file.text'

    def test_check_minio_multiple_match(self, mocker):
        response = read_json_file('list-objects-v2-minio-multiple-match.json')
        mock_client = mock_s3_client_list_objects_responder(mocker, response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'source': {
                'filters': {
                    'regexp': r'file.te?xt',
                    'version': 'every'
                }
            }
        }
        result = action_check.action_check(make_stream(input))
        assert len(result) == 2

    def test_check_aws_pretend_module_ids_are_versions_named_capture(self, mocker):
        response = read_json_file('list-objects-v2-full-bucket.json')
        mock_client = mock_s3_client_list_objects_responder(mocker, response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'source': {
                'filters': {
                    'regexp': r'book/m(?P<version>\d+)/.*',
                    'version': 'every'
                }
            }
        }
        result = action_check.action_check(make_stream(input))
        assert len(result) == 1259

    def test_check_aws_pretend_module_ids_are_versions_multi_group_capture(self, mocker):
        response = read_json_file('list-objects-v2-full-bucket.json')
        mock_client = mock_s3_client_list_objects_responder(mocker, response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'source': {
                'filters': {
                    'regexp': r'(book)/m(?P<version>\d+)/.*',
                    'version': '0'
                }
            }
        }
        result = action_check.action_check(make_stream(input))
        assert len(result) == 1259

    def test_check_aws_pretend_module_ids_are_versions_unnamed_capture(self, mocker):
        response = read_json_file('list-objects-v2-full-bucket.json')
        mock_client = mock_s3_client_list_objects_responder(mocker, response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'source': {
                'filters': {
                    'regexp': r'book/m(\d+)/.*',
                    'version': 'every'
                }
            }
        }
        result = action_check.action_check(make_stream(input))
        assert len(result) == 1259

    def test_check_aws_pretend_module_ids_are_versions_threshold(self, mocker):
        response = read_json_file('list-objects-v2-full-bucket.json')
        mock_client = mock_s3_client_list_objects_responder(mocker, response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'source': {
                'filters': {
                    'regexp': r'book/m(?P<version>\d+)/.*',
                    'version': '63000'
                }
            }
        }
        result = action_check.action_check(make_stream(input))
        assert len(result) == 13

    def test_check_aws_pretend_module_ids_are_versions_latest(self, mocker):
        response = read_json_file('list-objects-v2-full-bucket.json')
        mock_client = mock_s3_client_list_objects_responder(mocker, response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'source': {
                'filters': {
                    'regexp': r'book/m(?P<version>\d+)/.*',
                    'version': 'latest'
                }
            }
        }
        result = action_check.action_check(make_stream(input))
        assert len(result) == 1
        assert 'm63248' in result[0]['Key']
