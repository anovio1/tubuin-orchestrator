from typing import Optional
import paramiko
import logging
import os
from pathlib import Path
import gzip
import shutil

logger = logging.getLogger(__name__)


def safe_remove_local(path: Path):
    """Safely removes a local file, ignoring if it doesn't exist."""
    try:
        if path and path.exists():
            path.unlink()
            logger.info(f"Cleaned up local file: {path}")
    except Exception as e:
        logger.warning(f"Failed to clean up local file {path}: {e}")


def safe_remove_remote(sftp: paramiko.SFTPClient, remote_path: str):
    """Safely removes a remote file, ignoring if it doesn't exist."""
    if not remote_path:
        return
    try:
        sftp.remove(remote_path)
        logger.info(f"Cleaned up remote file: {remote_path}")
    except FileNotFoundError:
        pass 
    except Exception as e:
        if "Socket is closed" not in str(e):
            logger.warning(f"Failed to clean up remote file {remote_path}: {e}")


def compress_to_gz(local_path: Path) -> Path:
    """Compresses a file and returns the path to the new .gz file."""
    gz_path = local_path.with_suffix(local_path.suffix + ".gz")
    try:
        with open(local_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        logger.info(f"Compressed {local_path} -> {gz_path}")
        return gz_path
    except Exception as e:
        logger.error(f"Failed to gzip local file {local_path}: {e}")
        raise


def execute_remote_cmd(ssh: paramiko.SSHClient, cmd: str, desc: str = ""):
    """Executes a remote command, raising RuntimeError on failure."""
    logger.info(f"Executing remote: {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=300)
    exit_status = stdout.channel.recv_exit_status()
    if exit_status != 0:
        err = stderr.read().decode().strip()
        error_msg = (
            f"Remote {desc or 'cmd'} failed with exit status {exit_status}: {err}"
        )
        logger.error(error_msg)
        raise RuntimeError(error_msg)


def upload_gzipped_and_decompress_remotely(
    sftp: paramiko.SFTPClient,
    ssh: paramiko.SSHClient,
    local_path: Path,
    remote_path: Path,
):
    """
    Robustly uploads a file using a gzip strategy with guaranteed cleanup.
    This version uses the SSH 'mv' command for the final, atomic rename,
    bypassing any SFTP server rename restrictions.
    """
    if not local_path.is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    local_gz_path: Optional[Path] = None
    remote_gz_temp_path: Optional[Path] = None
    remote_uncompressed_path_str: Optional[str] = None

    try:
        # 1. Define
        rand_hex = os.urandom(4).hex()
        remote_gz_temp_path = remote_path.with_name(
            f".{remote_path.name}.{rand_hex}.tmp.gz"
        )
        remote_gz_temp_path_str = remote_gz_temp_path.as_posix()
        local_gz_path = compress_to_gz(local_path)

        # 2. Upload
        sftp.put(str(local_gz_path), remote_gz_temp_path_str)
        logger.info(f"Uploaded compressed file to {remote_gz_temp_path_str}")

        # 3. Decompress
        decompress_cmd = f"gunzip {remote_gz_temp_path_str}"
        execute_remote_cmd(ssh, decompress_cmd, "in-place decompression")
        remote_uncompressed_path_str = remote_gz_temp_path.with_suffix("").as_posix()
        logger.info(f"File decompressed on server to: {remote_uncompressed_path_str}")

        # 4. mv overwrite
        move_cmd = f"mv -f {remote_uncompressed_path_str} {remote_path.as_posix()}"
        execute_remote_cmd(ssh, move_cmd, "final move/rename")

        logger.info(f"Upload+decompress successful: {local_path} -> {remote_path}")

    except Exception:
        logger.exception("Gzipped SFTP upload/decompress failed")
        raise
    finally:
        logger.info("Executing final cleanup...")

        # 1. Clean up local compressed file
        if local_gz_path:
            safe_remove_local(local_gz_path)

        # 2. Clean up any potential remaining items on remote
        if remote_gz_temp_path:
            safe_remove_remote(sftp, remote_gz_temp_path.as_posix())

        if remote_uncompressed_path_str:
            safe_remove_remote(sftp, remote_uncompressed_path_str)
