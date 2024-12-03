import os
import paramiko
import logging
import time
import csv
import socket
from datetime import datetime
from typing import Optional, List, Dict
from paramiko.ssh_exception import SSHException
from logging.handlers import RotatingFileHandler


# Configure logging
def setup_logger(log_file="sftp_download.log"):
    # Create logger
    logger = logging.getLogger("SFTPDownloader")
    logger.setLevel(logging.INFO)

    # Create file handler with rotation (10MB max size, keep 5 backup files)
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5  # 10MB
    )

    # Create console handler
    console_handler = logging.StreamHandler()

    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Initialize logger
logger = setup_logger()


class SFTPStateManager:
    def __init__(self, state_file_path: str = "sftp_state.csv"):
        self.state_file_path = state_file_path
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        """Load the state from CSV file or create new if doesn't exist"""
        state = {"downloaded_files": {}}
        try:
            if os.path.exists(self.state_file_path):
                with open(self.state_file_path, "r", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        remote_path = row["remote_path"]
                        state["downloaded_files"][remote_path] = {
                            "mtime": float(row["mtime"]),
                            "size": int(row["size"]),
                            "last_download": row["last_download"],
                        }
        except Exception as e:
            logger.error(f"Error loading state file: {e}")
        return state

    def save_state(self):
        """Save the current state to CSV file"""
        try:
            fieldnames = ["remote_path", "mtime", "size", "last_download"]
            with open(self.state_file_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for remote_path, info in self.state["downloaded_files"].items():
                    writer.writerow(
                        {
                            "remote_path": remote_path,
                            "mtime": info["mtime"],
                            "size": info["size"],
                            "last_download": info["last_download"],
                        }
                    )
        except Exception as e:
            logger.error(f"Error saving state file: {e}")

    def is_file_downloaded(self, remote_path: str, file_attrs) -> bool:
        """Check if file was already downloaded based on modification time and size"""
        if remote_path in self.state["downloaded_files"]:
            stored_info = self.state["downloaded_files"][remote_path]
            current_mtime = file_attrs.st_mtime
            current_size = file_attrs.st_size

            return (
                stored_info["mtime"] == current_mtime
                and stored_info["size"] == current_size
            )
        return False

    def mark_file_downloaded(self, remote_path: str, file_attrs):
        """Mark a file as downloaded with its attributes"""
        self.state["downloaded_files"][remote_path] = {
            "mtime": file_attrs.st_mtime,
            "size": file_attrs.st_size,
            "last_download": datetime.now().isoformat(),
        }
        self.save_state()


def get_host_key(hostname: str, port: int = 22) -> Optional[paramiko.PKey]:
    """Get remote host key"""
    try:
        transport = paramiko.Transport((hostname, port))
        transport.start_client()
        key = transport.get_remote_server_key()
        transport.close()
        return key
    except Exception as e:
        logger.error(f"Failed to get host key: {str(e)}")
        return None


def download_all_files(
    hostname: str,
    username: str,
    password: str,
    remote_dir: str = "/",
    port: int = 22,
    local_folder: str = "sftp_download",
    max_retries: int = 3,
    retry_delay: int = 5,
    state_manager: Optional[SFTPStateManager] = None,
) -> List[str]:
    """
    Downloads all files from a remote SFTP directory with state management.
    Returns a list of successfully downloaded file paths.
    """
    if state_manager is None:
        state_manager = SFTPStateManager()

    downloaded_files = []
    ssh_client = paramiko.SSHClient()

    try:
        host_key = get_host_key(hostname, port)
        if host_key:
            hostkey_entry = (hostname, host_key.get_name(), host_key)
            ssh_client.get_host_keys().add(*hostkey_entry)
        else:
            logger.warning("Could not get host key, falling back to AutoAddPolicy")
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    except Exception as e:
        logger.warning(
            f"Error handling host key: {str(e)}, falling back to AutoAddPolicy"
        )
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    for attempt in range(max_retries):
        try:
            logger.info(f"Connection attempt {attempt + 1} of {max_retries}")

            ssh_client.connect(
                hostname,
                port,
                username,
                password,
                timeout=30,
                banner_timeout=60,
                auth_timeout=60,
                allow_agent=False,
                look_for_keys=False,
            )

            transport = ssh_client.get_transport()
            transport.set_keepalive(30)

            sftp = ssh_client.open_sftp()

            # Create local download folder if it doesn't exist
            os.makedirs(local_folder, exist_ok=True)

            def download_with_progress(sftp, remote_path, local_path, file_attrs):
                try:
                    # Check if file was already downloaded
                    if state_manager.is_file_downloaded(remote_path, file_attrs):
                        logger.info(f"Skipping {remote_path} - already downloaded")
                        return True

                    total_size = file_attrs.st_size
                    bytes_downloaded = 0
                    chunk_size = 32768

                    with sftp.open(remote_path, "rb") as remote_file:
                        with open(local_path, "wb") as local_file:
                            while True:
                                chunk = remote_file.read(chunk_size)
                                if not chunk:
                                    break
                                local_file.write(chunk)
                                bytes_downloaded += len(chunk)
                                progress = (bytes_downloaded / total_size) * 100
                                logger.info(
                                    f"Download progress for {os.path.basename(remote_path)}: {progress:.2f}%"
                                )

                    # Mark file as downloaded in state
                    state_manager.mark_file_downloaded(remote_path, file_attrs)
                    return True
                except Exception as e:
                    logger.error(f"Error downloading {remote_path}: {str(e)}")
                    return False

            def download_directory(sftp, remote_dir, local_dir):
                """Recursively download directory contents"""
                try:
                    items = sftp.listdir_attr(remote_dir)

                    for item in items:
                        remote_path = f"{remote_dir}/{item.filename}"
                        local_path = os.path.join(local_dir, item.filename)

                        if stat.S_ISDIR(item.st_mode):
                            logger.info(f"Creating directory: {local_path}")
                            os.makedirs(local_path, exist_ok=True)
                            download_directory(sftp, remote_path, local_path)
                        else:
                            logger.info(f"Processing file: {remote_path}")
                            if download_with_progress(
                                sftp, remote_path, local_path, item
                            ):
                                downloaded_files.append(local_path)

                except Exception as e:
                    logger.error(f"Error accessing directory {remote_dir}: {str(e)}")

            # Start the download process
            download_directory(sftp, remote_dir, local_folder)

            if downloaded_files:
                logger.info(f"Successfully downloaded {len(downloaded_files)} files")
                return downloaded_files
            else:
                logger.warning("No new files were downloaded")
                return []

        except (SSHException, TimeoutError, ConnectionError, socket.error) as e:
            if attempt < max_retries - 1:
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue
            else:
                logger.error(f"All connection attempts failed: {str(e)}")
                return []
        except Exception as e:
            logger.error(f"An unexpected error occurred: {str(e)}")
            return []
        finally:
            if "sftp" in locals():
                sftp.close()
            if ssh_client:
                ssh_client.close()

    return downloaded_files


if __name__ == "__main__":
    import stat  # Required for checking if path is directory

    # Get configuration from environment variables
    SFTP_HOSTNAME = os.environ.get("SFTP_HOSTNAME")
    SFTP_USERNAME = os.environ.get("SFTP_USERNAME")
    SFTP_PASSWORD = os.environ.get("SFTP_PASSWORD")
    REMOTE_DIR = os.environ.get("REMOTE_DIR", "/")
    LOCAL_FOLDER = os.environ.get("LOCAL_FOLDER")
    STATE_FILE = os.environ.get("STATE_FILE", "sftp_state.csv")

    logger.info(f"Attempting to connect to: {SFTP_HOSTNAME}")

    if not all([SFTP_HOSTNAME, SFTP_USERNAME, SFTP_PASSWORD]):
        logger.error("Missing required environment variables")
        exit(1)

    # Initialize state manager with CSV file
    state_manager = SFTPStateManager(STATE_FILE)

    downloaded_files = download_all_files(
        hostname=SFTP_HOSTNAME,
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        remote_dir=REMOTE_DIR,
        local_folder=LOCAL_FOLDER,
        state_manager=state_manager,
    )

    if downloaded_files:
        logger.info("Successfully downloaded files:")
        for file in downloaded_files:
            logger.info(f"- {file}")
    else:
        logger.info("No new files to download")
