# tubuin\config\sftp_conn_info.py
import os
from dotenv import load_dotenv

load_dotenv()
print("Loaded SFTP_PRIVATE_KEY_PATH =", os.environ.get("SFTP_PRIVATE_KEY_PATH"))


SFTP_CONN_INFO = {
    "HOST": os.environ["SFTP_HOST"],
    "PORT": int(os.environ["SFTP_PORT"]) if os.environ["SFTP_PORT"].isdigit() else 22,
    "USER": os.environ["SFTP_USER"],
    "PRIVATE_KEY_PATH": os.path.expanduser(os.environ["SFTP_PRIVATE_KEY_PATH"]),
}