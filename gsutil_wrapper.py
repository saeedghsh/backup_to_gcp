"""Wrappers for backup to GCP using gsutil"""
import os
from typing import List, Optional
import logging
import subprocess
import select


def _run_command(
    command: List[str], logger: logging.Logger, description: str = ""
) -> Optional[str]:
    logger.info(f"Running: {' '.join(command)}")
    try:
        result = subprocess.run(
            command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        logger.info(f"Successfully completed {description}")
        logger.info(f"Output:\n{result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(
            f"Error while executing {description}. Return code: {e.returncode}, Error: {e.stderr}"
        )
        return None


def authenticate_with_service_account(key_file_path: str, logger: logging.Logger):
    """Authenticate using gcloud and service account file key"""
    # Set GOOGLE_APPLICATION_CREDENTIALS environment variable
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path
    logger.info(f"Set GOOGLE_APPLICATION_CREDENTIALS environment variable to {key_file_path}")

    # Activate the service account with gcloud
    command = ["gcloud", "auth", "activate-service-account", "--key-file=" + key_file_path]
    _run_command(command, logger, "Activating service account with gcloud")

    # Reset environment variable
    del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]


def set_project_id(project_id: str, logger: logging.Logger):
    """Set project ID in .boto file"""
    # Get the path to the .boto configuration file
    boto_path = "/home/saeed/.boto"

    # Read the file and update the project ID
    with open(boto_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    with open(boto_path, "w", encoding="utf-8") as f:
        for line in lines:
            if line.startswith("project_id ="):
                f.write(f"project_id = {project_id}\n")
            else:
                f.write(line)
    logger.info(f"Updated project_id in {boto_path} to {project_id}")


def gsutil_rsync_wrapper(directory: str, bucket: str, logger: logging.Logger, operation: str):
    """Use gsutil rsync to perform various operations between local directory and GCS bucket.

    NOTE: Requiers installation of the gsutil and authentication outside this script.

    -n:      dry-run
    -d:      delete on remote, so that local and remote are in sync
    -c:      compute and compare checksum
    -r:      recursive
    -x:      exlude files "Desktop.ini", "FolderMarker.ico" or starting with ""._"
    -m:      execute in parallel (NOTE: this is on "gsutil" level, not "rsync")

    NOTE: Do not specify -u. It only takes size and time into consideration and takes
          precedence over -c, meaning checksum will be ignore when skipping existing files.
    """
    cmd = ["gsutil", "-m", "rsync"]
    cmd.extend(["-c"])
    cmd.extend(["-r"])
    cmd.extend(["-x", r"Desktop\.ini$|FolderMarker\.ico$|^\._.*"])
    if operation == "verify":
        cmd.append("-n")
    elif operation == "copy":
        pass
    elif operation == "sync":
        cmd.append("-d")
    else:
        logger.error(f"Unsupported operation: {operation}")
        return
    cmd.extend([directory, f"gs://{bucket}/"])

    os.environ["GSUTIL_LOG_LEVEL"] = "INFO"

    try:
        with subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True
        ) as process:
            while True:
                rlist, _, _ = select.select([process.stdout, process.stderr], [], [])
                for stream in rlist:
                    line = stream.readline()
                    if line:
                        if stream is process.stdout:
                            logger.info(line.strip())
                        else:
                            logger.warning(line.strip())  # Use WARNING for stderr

                # Check for termination
                if process.poll() is not None:
                    for line in process.stdout:
                        logger.info(line.strip())
                    for line in process.stderr:
                        logger.warning(line.strip())  # Use WARNING for stderr
                    break

        if process.returncode != 0:
            logger.error(f"Error executing gsutil rsync with return code {process.returncode}")

    except FileNotFoundError:
        logger.error("Command not found")
    except OSError as e:
        logger.error(f"OS error occurred: {e}")
    except ValueError:
        logger.error("Invalid arguments provided to Popen")

    del os.environ["GSUTIL_LOG_LEVEL"]
