# pylint: disable=missing-module-docstring
# pylint: disable=missing-function-docstring
import logging
from unittest.mock import Mock, patch, call
import pytest
from google.cloud import storage
from google.oauth2 import service_account
from utilities.google_cloud_wrapper import (
    _get_directory_size, _compute_sha256, get_client, copy_directory_to_gcs
)


def test_get_directory_size(tmp_path):
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    
    file1 = tmp_path / "file1.txt"
    file1.write_text("Hello, world!")  # 13 bytes

    file2 = subdir / "file2.txt"
    file2.write_text("Another file in a subdirectory.")  # 31 bytes

    file3 = subdir / "file3.txt"
    file3.write_text("Yet another file.")  # 17 bytes

    expected_size = 13 + 31 + 17
    assert _get_directory_size(tmp_path) == expected_size


@pytest.fixture
def sample_file(tmp_path):
    data = b"Hello, world!"
    p = tmp_path / "sample.txt"
    p.write_bytes(data)
    return p


def test_compute_sha256(sample_file):  # pylint: disable=redefined-outer-name
    # SHA-256 hash for "Hello, world!"
    expected_hash = "315f5bdb76d078c43b8ac0064e4a0164612b1fce77c869345bfc94c75894edd3"
    computed_hash = _compute_sha256(str(sample_file))
    assert computed_hash == expected_hash


def test_get_client():
    mock_credentials = Mock(spec=service_account.Credentials)
    with patch.object(service_account.Credentials, 'from_service_account_file', return_value=mock_credentials):
        mock_client_instance = Mock(spec=storage.Client)
        with patch.object(storage, 'Client', return_value=mock_client_instance):
            result = get_client("mock_credential_file.json", "mock_project_id")
            service_account.Credentials.from_service_account_file.assert_called_once_with("mock_credential_file.json")
            storage.Client.assert_called_once_with(credentials=mock_credentials, project="mock_project_id")
            assert result == mock_client_instance


@pytest.fixture
def setup_mocks():
    # Create a mock logger
    mock_logger = Mock(spec=logging.Logger)

    # Mock storage.Client and related methods
    mock_client = Mock(spec=storage.Client)
    mock_bucket = Mock()
    mock_blob = Mock()
    mock_blob.metadata = {}
    mock_client.get_bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    return mock_client, mock_logger, mock_bucket, mock_blob


def test_copy_directory_to_gcs(setup_mocks):  # pylint: disable=redefined-outer-name
    mock_client, mock_logger, mock_bucket, mock_blob = setup_mocks
    with patch("utilities.google_cloud_wrapper._get_directory_size", return_value=5000), \
         patch("utilities.google_cloud_wrapper._compute_sha256", return_value="mockhash"), \
         patch("os.walk", return_value=[("mock_dir", [], ["file1.txt"])]), \
         patch("os.path.join", return_value="mock_dir/file1.txt"), \
         patch("os.path.getsize", return_value=1000), \
         patch("os.path.relpath", return_value="file1.txt"):
        
        copy_directory_to_gcs("mock_dir", "mock_bucket", mock_client, mock_logger)

        mock_client.get_bucket.assert_called_once_with("mock_bucket")
        mock_bucket.blob.assert_called_once_with("file1.txt")
        mock_blob.upload_from_filename.assert_called_once_with("mock_dir/file1.txt")
        mock_logger.info.assert_has_calls([
            call("Uploaded mock_dir/file1.txt to file1.txt"),
            call("Finished uploading directory mock_dir to mock_bucket.")
        ])
        assert mock_logger.error.call_count == 0
