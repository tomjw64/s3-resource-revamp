import pytest

from src.action_in import action_in

from .helpers import (read_json_file_as_dict,
                      make_stream,
                      mock_s3_client)


DIR_NO_EXIST = '_tmp-delete-me'


class TestIn:
    def test_in_empty_bucket(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-empty.json')
        mock_client = mock_s3_client(mocker, list_response=response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'version': {
                'key': 'file.txt'
            },
            'params': {
                'mode': 'single'
            }
        }
        action_in(DIR_NO_EXIST, make_stream(input))

        mock_client.download_file.assert_called_once_with(
            Bucket=None,
            Key='file.txt',
            Filename=f'{DIR_NO_EXIST}/file.txt')

    def test_in_exact_match_latest(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-minio.json')
        mock_client = mock_s3_client(mocker, list_response=response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'version': {
                'key': 'file.txt'
            },
            'source': {
                'filters': {
                    'regexp': 'file.txt'
                }
            },
            'params': {
                'mode': 'single'
            }
        }
        action_in(DIR_NO_EXIST, make_stream(input))

        mock_client.download_file.assert_called_once_with(
            Bucket=None,
            Key='file.txt',
            Filename=f'{DIR_NO_EXIST}/file.txt')

    def test_in_bad_version_single(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-minio.json')
        mock_client = mock_s3_client(mocker, list_response=response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'version': {
                'key': 'file.txt'
            },
            'source': {
                'filters': {
                    'regexp': 'file.txt',
                    'version': 'asdf'
                }
            },
            'params': {
                'mode': 'single'
            }
        }
        action_in(DIR_NO_EXIST, make_stream(input))

        mock_client.download_file.assert_called_once_with(
            Bucket=None,
            Key='file.txt',
            Filename=f'{DIR_NO_EXIST}/file.txt')

    def test_in_bad_version_all(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-minio.json')
        mock_client = mock_s3_client(mocker, list_response=response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'version': {
                'key': 'file.txt'
            },
            'source': {
                'filters': {
                    'regexp': 'file.txt',
                    'version': 'asdf'
                }
            },
            'params': {
                'mode': 'all'
            }
        }
        with pytest.raises(ValueError):
            action_in(DIR_NO_EXIST, make_stream(input))

    def test_in_multiple_match_latest(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-minio-multiple-match.json')
        mock_client = mock_s3_client(mocker, list_response=response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'version': {
                'key': 'file.text'
            },
            'source': {
                'filters': {
                    'regexp': r'file.te?xt',
                    'version': 'latest'
                }
            },
            'params': {
                'mode': 'all'
            }
        }
        action_in(DIR_NO_EXIST, make_stream(input))

        mock_client.download_file.assert_called_once_with(
            Bucket=None,
            Key='file.text',
            Filename=f'{DIR_NO_EXIST}/file.text')

    def test_in_multiple_match(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-minio-multiple-match.json')
        mock_client = mock_s3_client(mocker, list_response=response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'version': {
                'key': 'file.text'
            },
            'source': {
                'filters': {
                    'regexp': r'file.te?xt',
                    'version': 'every'
                }
            },
            'params': {
                'mode': 'all'
            }
        }
        action_in(DIR_NO_EXIST, make_stream(input))
        assert mock_client.download_file.call_count == 2

    def test_in_pretend_module_ids_are_versions_named_capture(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-full-bucket.json')
        mock_client = mock_s3_client(mocker, list_response=response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'version': {
                'key': 'book/m63248/index.cnxml'
            },
            'source': {
                'filters': {
                    'regexp': r'book/m(?P<version>\d+)/index[.].*',
                    'version': 'every'
                }
            },
            'params': {
                'mode': 'all'
            }
        }
        action_in(DIR_NO_EXIST, make_stream(input))
        assert mock_client.download_file.call_count == 159

    def test_in_pretend_module_ids_are_versions_named_capture_single(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-full-bucket.json')
        mock_client = mock_s3_client(mocker, list_response=response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'version': {
                'key': 'book/m63248/index.cnxml'
            },
            'source': {
                'filters': {
                    'regexp': r'book/m(?P<version>\d+)/index[.].*',
                    'version': 'every'
                }
            },
            'params': {
                'mode': 'single'
            }
        }
        action_in(DIR_NO_EXIST, make_stream(input))

        mock_client.download_file.assert_called_once_with(
            Bucket=None,
            Key='book/m63248/index.cnxml',
            Filename=f'{DIR_NO_EXIST}/book/m63248/index.cnxml')

    def test_in_pretend_module_ids_are_versions_multi_group_capture(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-full-bucket.json')
        mock_client = mock_s3_client(mocker, list_response=response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'version': {
                'key': 'book/m63248/index.cnxml'
            },
            'source': {
                'filters': {
                    'regexp': r'book/m(?P<version>\d+)/index[.].*',
                    'version': '0'
                }
            },
            'params': {
                'mode': 'all'
            }
        }
        action_in(DIR_NO_EXIST, make_stream(input))
        assert mock_client.download_file.call_count == 159

    def test_in_pretend_module_ids_are_versions_threshold(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-full-bucket.json')
        mock_client = mock_s3_client(mocker, list_response=response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'version': {
                'key': 'book/m63248/index.cnxml'
            },
            'source': {
                'filters': {
                    'regexp': r'book/m(?P<version>\d+)/index[.].*',
                    'version': '63000'
                }
            },
            'params': {
                'mode': 'all'
            }
        }
        action_in(DIR_NO_EXIST, make_stream(input))
        assert mock_client.download_file.call_count == 2

    def test_in_pretend_module_ids_are_versions_latest(self, mocker):
        response = read_json_file_as_dict('list-objects-v2-full-bucket.json')
        mock_client = mock_s3_client(mocker, list_response=response)
        mocker.patch('boto3.client', return_value=mock_client)
        input = {
            'version': {
                'key': 'book/m63248/index.cnxml'
            },
            'source': {
                'filters': {
                    'regexp': r'book/m(?P<version>\d+)/index[.].*',
                    'version': 'latest'
                }
            },
            'params': {
                'mode': 'all'
            }
        }
        action_in(DIR_NO_EXIST, make_stream(input))

        mock_client.download_file.assert_called_once_with(
            Bucket=None,
            Key='book/m63248/index.cnxml',
            Filename=f'{DIR_NO_EXIST}/book/m63248/index.cnxml')
