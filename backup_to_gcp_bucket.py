"""Entry point for backup to Google Cloud Plotform (storage bucket)"""
import argparse
from datetime import datetime

from logging_wrapper import setup_logging
from google_cloud_wrapper import copy_directory_to_gcs, get_client
from gsutil_wrapper import (
    set_project_id,
    authenticate_with_service_account,
    gsutil_rsync_wrapper,
)


def _parse_arguments() -> argparse.Namespace:
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
        choices=["copy", "sync"],
        default="sync",
        help="Operation mode",
    )
    parser.add_argument(
        "--use-gsutil",
        dest="use_gsutil",
        action="store_true",
        default=False,
        help="Use gsutil rsync (instead of python implementation based on google packages)",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Just do dry run, don't actually upload/delete",
    )
    args = parser.parse_args()
    return args


def main():  # pylint: disable=missing-function-docstring
    arguments = _parse_arguments()
    directory = arguments.directory
    project_id = arguments.project_id
    bucket_name = arguments.bucket
    credential_file = arguments.credential_file
    operation = arguments.operation
    use_gsutil = arguments.use_gsutil
    dry_run = arguments.dry_run

    # Setup logger

    log_filename = f"backup_log_operation_{operation}"
    log_filename += "_dryrun" if dry_run else ""
    log_filename += f"_{bucket_name}.log"
    logger = setup_logging(log_filename=log_filename, directory="logs")

    # Start the backup process
    logger.info("Starting backup for: %s", directory)
    if use_gsutil:
        set_project_id(project_id, logger)
        authenticate_with_service_account(credential_file, logger)
        start_time = datetime.now()
        gsutil_rsync_wrapper(directory, bucket_name, logger, operation, dry_run)

    else:
        if operation != "copy":
            logger.error("Operation %s is only supported with gsutil (right now)", operation)
            return
        if dry_run:
            logger.error("Dry run is only supported with gsutil (right now)")
            return
        client = get_client(credential_file, project_id)
        start_time = datetime.now()
        copy_directory_to_gcs(directory, bucket_name, client, logger)

    end_time = datetime.now()
    elapsed_time = end_time - start_time
    logger.info("Backup process finished. Total elapsed time: %f", elapsed_time.total_seconds())


if __name__ == "__main__":
    main()
