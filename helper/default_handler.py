"""Collection of default helper functions."""

from pathlib import Path


def create_filedir(filedir_path: Path, *args) -> Path:
    """
    Create a file or directory at the specified path.

    This function creates either a directory or a file based on the arguments provided.
    If 'dir', 'directory', or 'folder' is present in args, it creates a directory.
    Otherwise, it creates a file along with any necessary parent directories.

    Args:
        filedir_path (Path): The path where the file or directory should be created.
        *args: Variable length argument list. If contains 'dir', 'directory', or 'folder',
            a directory will be created instead of a file.

    Returns:
        Path

    Note:
        - For directories: Creates all parent directories if they don't exist (parents=True)
        - For files: Creates parent directories and then creates an empty file
        - Both operations will not raise an error if the path already exists (exist_ok=True)
    """
    isdir = any(x in args for x in ("dir", "directory", "folder"))
    if isdir:
        filedir_path.mkdir(parents=True, exist_ok=True)
    else:
        filedir_path.parent.mkdir(parents=True, exist_ok=True)
        filedir_path.touch(exist_ok=True)
    return filedir_path
