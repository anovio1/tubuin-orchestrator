# tubuin\config\sftp_conn.py
from contextlib import contextmanager
from .sftp_conn_info import SFTP_CONN_INFO
import paramiko
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class ConnectionFactory:
    def __init__(self, conn_info: Dict[str, Any]):
        self._conn_info = conn_info
        self._validate_config()
        self._pkey = paramiko.RSAKey.from_private_key_file(
            self._conn_info["PRIVATE_KEY_PATH"]
        )

    def _validate_config(self):
        required_keys = ["HOST", "PORT", "USER", "PRIVATE_KEY_PATH"]
        missing = [k for k in required_keys if not self._conn_info.get(k)]
        if missing:
            raise ValueError(f"Missing SFTP configuration: {', '.join(missing)}")
        try:
            self._conn_info["PORT"] = int(self._conn_info["PORT"])
        except (ValueError, TypeError):
            raise ValueError(f"Invalid port value: {self._conn_info['PORT']}")

    def _create_base_ssh_client(self) -> paramiko.SSHClient:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        host = self._conn_info["HOST"]
        port = self._conn_info["PORT"]
        user = self._conn_info["USER"]

        logger.info(f"Connecting to {user}@{host}:{port}...")
        ssh_client.connect(
            hostname=host, port=port, username=user, pkey=self._pkey, timeout=30
        )

        transport = ssh_client.get_transport()
        if transport and transport.is_active():
            transport.set_keepalive(60)
            logger.info("SSH transport keepalive set to 60 seconds.")

        return ssh_client

    @contextmanager
    def get_sftp_ssh_context(self):
        ssh_client = self._create_base_ssh_client()
        try:
            sftp_client = ssh_client.open_sftp()
            yield sftp_client, ssh_client
        finally:
            logger.info("Closing SSH client and all associated sessions.")
            ssh_client.close()

    @contextmanager
    def get_sftp_context(self):
        ssh_client = self._create_base_ssh_client()
        try:
            sftp_client = ssh_client.open_sftp()
            yield sftp_client
        finally:
            logger.info("Closing SSH client and all associated sessions.")
            ssh_client.close()


sftp_connection_factory = ConnectionFactory(SFTP_CONN_INFO)


sftp_ssh_conn = sftp_connection_factory.get_sftp_ssh_context
sftp_conn = sftp_connection_factory.get_sftp_context
