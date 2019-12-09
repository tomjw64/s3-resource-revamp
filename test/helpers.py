import io
import json
from pathlib import Path


def get_data_dir():
    return Path(__file__).resolve().parent / 'data'


def read_json_file_as_dict(data_file):
    with open(get_data_dir() / data_file) as f:
        try:
            return json.load(f)
        except json.decoder.JSONDecodeError:
            return {}


def make_stream(json_compat_dict):
    stream = io.StringIO()
    json.dump(json_compat_dict, stream)
    stream.seek(0)
    return stream


def mock_s3_client(mocker, *, list_response=None):
    mock_client = mocker.Mock()
    mock_client.list_objects_v2 = mocker.Mock(return_value=list_response)
    mock_client.download_file = mocker.Mock()
    mock_client.upload_file = mocker.Mock()
    return mock_client
