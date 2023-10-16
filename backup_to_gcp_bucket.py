import os
import logging
import argparse
import hashlib
from datetime import datetime

from tqdm import tqdm

from google.cloud import storage
from google.oauth2 import service_account

from logging_helper import setup_logging


def get_directory_size(directory_path: str) -> int:
    """Recursively calculates the size of a directory."""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def compute_sha256(file_path: str) -> str:
    """Compute SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def backup_directory_to_gcs(
        directory_path: str,
        bucket_name: str,
        client: storage.Client,
        operation: str,
        logger: logging.Logger,
    ):
    """Backup local directory to a GCS bucket."""
    bucket = client.get_bucket(bucket_name)
    directory_size = get_directory_size(directory_path)
    uploaded_size = 0

    with tqdm(total=directory_size, unit="B", unit_scale=True, unit_divisor=1024) as pbar:
        for foldername, _, filenames in os.walk(directory_path):
            for filename in filenames:
                file_path = os.path.join(foldername, filename)
                blob_path = os.path.relpath(file_path, directory_path)
                blob = bucket.blob(blob_path)

                try:
                    local_hash = compute_sha256(file_path)
                except Exception as e:
                    logger.error(f"Error computing hash for {file_path}: {e}")
                    continue

                blob_hash = None
                if blob.exists():
                    blob.reload()  # Fetch blob metadata
                    blob_hash = blob.metadata.get('sha256') if blob.metadata else None

                if local_hash != blob_hash:
                    try:
                        blob.metadata = {'sha256': local_hash}
                        blob.upload_from_filename(file_path)
                        file_size = os.path.getsize(file_path)
                        uploaded_size += file_size
                        pbar.update(file_size)
                        logger.info(f"Uploaded {file_path} to {blob_path}")
                    except Exception as e:
                        logger.error(f"Error uploading {file_path}: {e}")
                else:
                    logger.info(f"HASH match, skip uploading {file_path}")

                pbar.update(os.path.getsize(file_path))

    logger.info(f"Finished uploading directory {directory_path} to {bucket_name}.")
    print(f"Backup of {directory_path} completed!")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--directory",
        type=str,
        required=True,
        help="Directory to backup",
    )
    parser.add_argument(
        "--project-id",
        type=str,
        required=True,
        help="Project ID",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help="Bucket name",
    )
    parser.add_argument(
        "--credential-file",
        type=str,
        required=True,
        help="Service account cridential file",
    )
    parser.add_argument(
        "--operation",
        type=str,
        choices=["verify", "copy", "sync"],
        default="sync",
        help="Operation mode",
    )
    args = parser.parse_args()
    return args


def main():
    arguments = parse_arguments()
    directory = arguments.directory
    project_id = arguments.project_id
    bucket_name = arguments.bucket
    credential_file = arguments.credential_file
    operation = arguments.operation

    # Setup logger
    log_filename = f"backup_log_{bucket_name}.log"
    logger = setup_logging(log_filename=log_filename, directory="logs")

    # Authenticate using the service account key
    credentials = service_account.Credentials.from_service_account_file(credential_file)
    client = storage.Client(credentials=credentials, project=project_id)

    # Start the backup process
    print(f"Starting backup for: {directory}")
    start_time = datetime.now()
    backup_directory_to_gcs(directory, bucket_name, client, operation, logger)
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    logger.info(f"Backup process finished. Total elapsed time: {elapsed_time}")


if __name__ == "__main__":
    main()