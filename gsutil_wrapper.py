import logging
import subprocess


def _run_command(command: str, logger: logging.Logger, description: str=""):  # -> CompletedProcess[str]
    logger.info(f"Running: {' '.join(command)}")
    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(f"Successfully completed {description}")
        logger.info(f"Output:\n{result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Error while executing {description}. Return code: {e.returncode}, Error: {e.stderr}")
        return None


def authenticate_with_service_account(key_file_path: str, logger: logging.Logger):
    # Set GOOGLE_APPLICATION_CREDENTIALS environment variable
    command = ['export', f'GOOGLE_APPLICATION_CREDENTIALS={key_file_path}']
    _run_command(command, logger, "Setting GOOGLE_APPLICATION_CREDENTIALS environment variable")
    
    # Activate the service account with gcloud
    command = ['gcloud', 'auth', 'activate-service-account', '--key-file=' + key_file_path]
    _run_command(command, logger, "Activating service account with gcloud")


def set_project_id(project_id: str, logger: logging.Logger):
    # Get the path to the .boto configuration file
    boto_path = "~/.boto"
    
    # Read the file and update the project ID
    with open(boto_path, 'r') as f:
        lines = f.readlines()

    with open(boto_path, 'w') as f:
        for line in lines:
            if line.startswith('project_id ='):
                f.write(f'project_id = {project_id}\n')
            else:
                f.write(line)
    logger.info(f"Updated project_id in {boto_path} to {project_id}")


def gsutil_rsync_wrapper(
        directory: str,
        bucket: str,
        logger: logging.Logger,
        operation: str,
    ):
    """Use gsutil rsync to perform various operations between local directory and GCS bucket.

    NOTE: Requiers installation of the gsutil and authentication outside this script.

    -n:      dry-run
    -d:      delete on remote, so that local and remote are in sync
    -r:      recursive
    -P:      show progress
    -x:      exlude files "Desktop.ini", "FolderMarker.ico" or starting with ""._"
    -m -w 4: execute in parallel with 4 workers (CPU)
    -L INFO: specify logging level.

    NOTE: Do not specify -u. It only takes size and time into consideration and takes
          precedence over -c, meaning checksum will be ignore when skipping existing files.
    """
    cmd = ["gsutil", "rsync"]
    cmd.extend([
        "-r -P -m -w 4 -L INFO",
        '-x "Desktop\.ini$|FolderMarker\.ico$|^\._.*"',
    ])
    if operation == "verify":
        cmd.append("-n")
    elif operation == "copy":
        pass
    elif operation == "sync":
        cmd.append("-d")
    else:
        logger.error(f"Unsupported operation: {operation}")
        return
    cmd.extend([directory, bucket])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if operation == "verify" and "Starting synchronization" in result.stdout:
            logger.info("\nDifferences detected:\n")
        logger.info(result.stdout)

    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing gsutil rsync: {e}")
        logger.error(e.stderr)