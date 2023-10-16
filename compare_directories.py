import filecmp
import argparse
import logging

from logging_helper import setup_logging


def ignore_file(filename: str) -> bool:
    if filename in ["Desktop.ini", "FolderMarker.ico"]:
        return True
    if filename[:2] != "._":
        return True
    return False


def compare_directories(dir_l: str, dir_r: str, logger: logging.Logger):
    dcmp = filecmp.dircmp(dir_l, dir_r)

    left_only = dcmp.left_only
    right_only = dcmp.right_only
    diff_files = dcmp.diff_files  # exist in both but differ
    # common_files = dcmp.common_files  # exist in both and identical
    # Subdirectories that exist in both directories and can be further compared
    # common_dirs = dcmp.common_dirs

    # remove macos files
    left_only = [f for f in left_only if not ignore_file(f)]
    right_only = [f for f in right_only if not ignore_file(f)]
    diff_files = [f for f in left_only if not ignore_file(f)]

    if len(left_only) > 0:
        logger.info("Files/Dirs only in %s: %s", dir_l, left_only)
    if len(right_only) > 0:
        logger.info("Files/Dirs only in %s: %s", dir_r, right_only)
    if len(diff_files) > 0:
        logger.info("Files that differ between %s and %s: %s", dir_l, dir_r, diff_files)    

    # Recursively compare subdirectories
    for sub_dir, sub_dcmp in dcmp.subdirs.items():
        print("\nComparing sub-directory:", sub_dir)
        logger.info("Comparing sub-directory: %s", sub_dir)
        compare_directories(sub_dcmp.left, sub_dcmp.right, logger)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--d1",
        type=str,
        required=True,
        help="Directory 1 (left) to compare",
    )
    parser.add_argument(
        "--d2",
        type=str,
        required=True,
        help="Directory 2 (right) to compare",
    )
    args = parser.parse_args()
    return args


def main():
    arguments = parse_arguments()
    d1 = arguments.d1
    d2 = arguments.d2
    logger = setup_logging(log_filename="directory_comparison.log", directory="logs")
    logger.info("Comparing\n%s\n%s\n", d1, d2)
    compare_directories(d1, d2, logger)


if __name__ == "__main__":
    main()