from pathlib import Path

from src.action_out import action_out

from .helpers import (make_stream,
                      mock_s3_client)


DIR_NO_EXIST = '_tmp-delete-me'


class TestOut:
    def test_upload_called(self, mocker):
        mock_client = mock_s3_client(mocker)
        mocker.patch('boto3.client', return_value=mock_client)
        mocker.patch.object(Path, 'glob', return_value=[Path(f'{DIR_NO_EXIST}/file.txt')])
        mocker.patch.object(Path, 'is_file', new=lambda path: '.' in str(path))
        input = {
            'params': {
                'upload_glob': '**/*.txt'
            }
        }
        action_out(DIR_NO_EXIST, make_stream(input))

        mock_client.upload_file.assert_called_once_with(
            Bucket=None,
            Key='file.txt',
            Filename=f'{DIR_NO_EXIST}/file.txt')

    def test_dirs_filtered(self, mocker):
        mock_client = mock_s3_client(mocker)
        mocker.patch('boto3.client', return_value=mock_client)
        mocker.patch.object(Path, 'glob', return_value=[
            Path(f'{DIR_NO_EXIST}/file.txt'),
            Path(f'{DIR_NO_EXIST}/some-dir')])
        mocker.patch.object(Path, 'is_file', new=lambda path: '.' in str(path))
        input = {
            'params': {
                'upload_glob': '**/*'
            }
        }
        action_out(DIR_NO_EXIST, make_stream(input))

        mock_client.upload_file.assert_called_once_with(
            Bucket=None,
            Key='file.txt',
            Filename=f'{DIR_NO_EXIST}/file.txt')
