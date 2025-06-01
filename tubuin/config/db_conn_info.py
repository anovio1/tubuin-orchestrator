# tubuin\config\db_conn_info.py
import os
from dotenv import load_dotenv
load_dotenv()

DB_CONN_INFO = (
    f"host={os.environ['DB_HOST']} "
    f"port={os.environ['DB_PORT']} "
    f"dbname={os.environ['DB_NAME']} "
    f"user={os.environ['DB_USER']} "
    f"password={os.environ['DB_PASS']}"
)