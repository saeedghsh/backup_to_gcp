import os
import hashlib
import logging
import tqdm

from google.cloud import storage


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


def copy_directory_to_gcs(
        directory_path: str,
        bucket_name: str,
        client: storage.Client,
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
