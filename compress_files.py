#!/usr/bin/env python3
import os
import shutil
import subprocess
import logging
import sqlite3
import time

# Logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# Constants
DB_PATH = '/Users/dstorey/Desktop/movie_processing/files.db'
DESTINATION_DIR = '/Users/dstorey/Desktop/movie_processing/'
HANDBRAKE_PRESET = '/Users/dstorey/Desktop/movie_processing/compress.json'

# Example usage
SMB_SERVER = "mnemosyn._smb._tcp.local"
SMB_PATH = "/media"
MOUNT_POINT = "/Users/dstorey/Desktop/movie_processing/media"

def is_smb_mounted(smb_server, smb_path, mount_point):
    try:
        result = subprocess.run(['mount'], stdout=subprocess.PIPE, text=True)
        output = result.stdout
        
        # Check if the SMB server and path are in the output
        if f"//GUEST:@{smb_server}{smb_path} on {mount_point}" in output:
            return True
        return False
    except Exception as e:
        logging.error(f"Error checking mount status: {e}")
        return False

def mount_smb(smb_server, smb_path, mount_point):
    try:
        if not os.path.exists(mount_point):
            os.makedirs(mount_point)
        # Construct the mount command
        cmd = ['sudo', 'mount_smbfs', f"//GUEST@{smb_server}{smb_path}", mount_point]
        logging.info(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        logging.info(f"Mounted //GUEST@{smb_server}{smb_path} on {mount_point}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error mounting SMB (exit status {e.returncode}): {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def ensure_smb_mounted(smb_server, smb_path, mount_point):
    if is_smb_mounted(smb_server, smb_path, mount_point):
        pass
    else:
        logging.info(f"//GUEST@{smb_server}{smb_path} is not mounted. Mounting now...")
        mount_smb(smb_server, smb_path, mount_point)

def scan_files(scan_path, size_threshold):
    logging.info("Scanning for new files to be added")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Create table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS files
                (path TEXT PRIMARY KEY, size INTEGER, processed BOOLEAN)''')

    # Recursively scan the directory and add files larger than size_threshold to the database
    for root, dirs, files in os.walk(scan_path):
        for file in files:
            try :
                ensure_smb_mounted(SMB_SERVER, SMB_PATH, MOUNT_POINT)
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                if file_size > size_threshold:
                    c.execute("INSERT OR IGNORE INTO files (path, size, processed) VALUES (?, ?, ?)",
                            (file_path, file_size, False))
            except Exception as e:
                logging.error(f"Error adding large file: {e}")
                pass

    conn.commit()
    conn.close()
    logging.info("Scan Completed")

def copy_with_retries(src, dst, retries=2):
    attempt = 0
    while attempt <= retries:
        try:
            shutil.copy2(src, dst)
            return True
        except Exception as e:
            logging.error(f"Copy attempt {attempt + 1} failed: {e}")
            attempt += 1
            time.sleep(1)  # Backoff before retry
    return False

def process_files():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Select an unprocessed file
    c.execute("SELECT rowid, path FROM files WHERE processed = 0 ORDER BY size DESC LIMIT 1")
    row = c.fetchone()

    while row:
        rowid, original_file_path = row
        try:
            logging.info(f"Beginning to process {os.path.basename(original_file_path)}")

            # Copy the file
            input_file = os.path.join(DESTINATION_DIR, os.path.basename(original_file_path))

            # Replace extension with .mp4
            base, ext = os.path.splitext(input_file)
            if ext.lower() == '.mp4':
                output_file = base + '.mp42'
                logging.info(f"Destination path {input_file} already ends with .mp4. Using .mp42 for temporary output.")
            else:
                output_file = base + '.mp4'

            
            logging.info(f"Following files identified:\n\t original_file - {original_file_path}\n\t input_file - {input_file}\n\t output_file - {output_file}")

            # Get a copy of the original file locally
            logging.info(f"Copying {os.path.basename(original_file_path)}")
            ensure_smb_mounted(SMB_SERVER, SMB_PATH, MOUNT_POINT)
            if not copy_with_retries(original_file_path, input_file):
                logging.error(f"Failed to copy {original_file_path} after multiple attempts")
                raise Exception(f"Failed to copy {original_file_path}")

            # Process the file with HandBrakeCLI
            handbrake_command = [
                'HandBrakeCLI',
                '--preset-import-gui', HANDBRAKE_PRESET,
                '-i', input_file,
                '-o', output_file
            ]

            logging.info(f"Running the following command {handbrake_command}")
            process = subprocess.Popen(handbrake_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            # Read and log output in real-time
            line_counter = 0
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    if "encoding" in output.lower():
                        line_counter += 1
                        if line_counter % 1000 == 0:
                            logging.info(output.strip())
                            continue
                    else:
                        logging.info(output.strip())
                        

            stderr = process.communicate()[1]
            if stderr:
                logging.error(stderr.strip())

            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, handbrake_command)

            # Remove the input_file
            os.remove(input_file)

            try:
                # Rename .mp42 back to .mp4 if necessary
                if output_file.endswith('.mp42'):
                    final_output_file = output_file.replace('.mp42', '.mp4')
                    os.rename(output_file, final_output_file)
                    output_file = final_output_file

                # Copy the output file to the original file directory and remove the original file
                logging.info(f"Processing of {os.path.basename(original_file_path)} completed, copying {output_file} to original directory")
                ensure_smb_mounted(SMB_SERVER, SMB_PATH, MOUNT_POINT)
                if not copy_with_retries(output_file, os.path.join(os.path.dirname(original_file_path), os.path.basename(output_file))):
                    logging.error(f"Failed to copy {output_file} back to original directory after multiple attempts")
                    raise Exception(f"Failed to copy {output_file}")

                # Remove the output file
                logging.info(f"Unlinking {output_file}")
                os.remove(output_file)

            except Exception as e:
                logging.error(f"Failed to manage the file {output_file}: {e}")

            try:
                logging.info(f"Unlinking {original_file_path}")
                ensure_smb_mounted(SMB_SERVER, SMB_PATH, MOUNT_POINT)
                os.remove(original_file_path)
            except Exception as e:
                logging.error(f"Failed to unlink the file {original_file_path}: {e}")

            # Update the database
            c.execute("UPDATE files SET processed = 1 WHERE rowid = ?", (rowid,))
            conn.commit()

        except Exception as e:
            logging.error(f"Failed to process file {original_file_path}: {e}")
            if os.path.exists(input_file):
                os.remove(input_file)  # Clean up any intermediate files
            if os.path.exists(output_file):
                os.remove(output_file)  # Clean up any output files in case of failure

        # Fetch the next unprocessed file
        c.execute("SELECT rowid, path FROM files WHERE processed = 0 ORDER BY size DESC LIMIT 1")
        row = c.fetchone()

    conn.close()

ensure_smb_mounted(SMB_SERVER, SMB_PATH, MOUNT_POINT)
# Scan movies greater than 15GB
scan_files(os.path.join(MOUNT_POINT, 'movies'), 15 * 1024 * 1024 * 1024)


# # # Scan tv shows greater than 5GB
scan_files(os.path.join(MOUNT_POINT, 'television'), 2 * 1024 * 1024 * 1024)

# Process files via handbrake 
process_files()
