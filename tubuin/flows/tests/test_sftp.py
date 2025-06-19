# tubuin/flows/test_sftp.py

import logging
from pathlib import Path

# 1. Import the new, required functions
from config.sftp_conn import sftp_ssh_conn
from logic.utils.sftp import upload_gzipped_and_decompress_remotely

# 2. Configure basic logging to see output from the utility functions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def upload_gzipped_test_file():
    """
    Tests the entire gzipped upload and remote decompression workflow.
    """
    local_test_file = Path("large_test_file.dat")
    remote_path = Path(f"/home/nodeuser/apps/go-api/private/{local_test_file.name}")
    file_size_mb = 20

    # 3. Create a more substantial test file to properly test the workflow
    print(f"--- Creating a {file_size_mb}MB local test file: {local_test_file} ---")
    try:
        with open(local_test_file, "wb") as f:
            # This is a fast way to create a large, compressible file
            f.write(b'\0' * (file_size_mb * 1024 * 1024))
        print("Local test file created successfully.")

        # 4. Use the new context manager that provides both SFTP and SSH clients
        with sftp_ssh_conn() as (sftp, ssh):
            print("\n--- SFTP/SSH connection established. Starting upload process... ---")

            # 5. Call the new orchestrator function
            upload_gzipped_and_decompress_remotely(
                sftp=sftp,
                ssh=ssh,
                local_path=local_test_file,
                remote_path=remote_path
            )
            print("\n--- Upload process completed successfully! ---")

    except Exception as e:
        # The utility functions have their own detailed logging.
        # This just prints a final status message for the test script.
        logging.error(f"The test script encountered an unhandled exception: {e}", exc_info=True)
        print(f"\n--- TEST FAILED. See logs for details. ---")

    finally:
        # 6. Ensure the local test file is always cleaned up
        if local_test_file.exists():
            local_test_file.unlink()
            print(f"\n--- Local test file '{local_test_file}' deleted. ---")


if __name__ == "__main__":
    upload_gzipped_test_file()