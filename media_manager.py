#!/usr/bin/env python3

import os
import sys
import json
import sqlite3
import platform
import shlex
import subprocess
import logging
import time
import shutil

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Constants
DB_PATH = '/Users/dstorey/Desktop/movie_processing/files.db'
SMB_SERVER = "mnemosyn._smb._tcp.local"
SMB_PATH = "/media"
MOUNT_POINT = "/Users/dstorey/Desktop/movie_processing/media"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
DB_WRITE_INTERVAL = 10  # Write to DB every 10 files processed
DESTINATION_DIR = '/Users/dstorey/Desktop/movie_processing/'
HANDBRAKE_PRESET = '/Users/dstorey/Desktop/movie_processing/compress.json'

VIDEO_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.webm', '.m4v', 
    '.mpg', '.mpeg', '.3gp', '.3g2', '.ogv', '.vob', '.rmvb', '.asf',
    '.m2ts', '.mts', '.ts', '.divx', '.f4v'
}


def setup_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()



    # Create media_files table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS media_files (
        file_path TEXT PRIMARY KEY,
        file_basename TEXT,
        file_size INTEGER,
        last_modified INTEGER,
        content_type TEXT,
        audio_metadata TEXT,
        subtitle_metadata TEXT,
        needs_compression BOOLEAN,
        has_been_reviewed BOOLEAN
    )
    ''')

    # Create metadata table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    ''')

    # Insert initial metadata
    try:
        cursor.execute("INSERT INTO metadata (key, value) VALUES (?, ?)", ("last_full_scan", "0"))
    except Exception as e:
        pass

    conn.commit()
    logging.info("Database schema created successfully")
    return conn

def create_db_connection(db_path):
    try:
        conn = setup_database(db_path)
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error setting up database: {e}")
        raise



def get_file_metadata(file_path):
    try:
        result = subprocess.run([
            'mediainfo',
            '--Output=JSON',
            file_path
        ], capture_output=True, text=True, check=True)

        data = json.loads(result.stdout)
        
        audio_tracks = []
        subtitle_tracks = []
        
        for track in data.get('media', {}).get('track', []):
            if track.get('@type') == 'Audio':
                audio_tracks.append({
                    'language': track.get('Language', 'Unknown'),
                    'format': track.get('Format', 'Unknown'),
                    'channels': track.get('Channels', 'Unknown'),
                    'bit_rate': track.get('BitRate', 'Unknown')
                })
            elif track.get('@type') == 'Text':
                subtitle_tracks.append({
                    'language': track.get('Language', 'Unknown'),
                    'format': track.get('Format', 'Unknown')
                })
        
        return audio_tracks, subtitle_tracks
    except subprocess.CalledProcessError:
        logging.error(f"Error: mediainfo failed for {file_path}")
        return [], []
    except Exception as e:
        logging.error(f"Error processing {file_path}: {str(e)}")
        return [], []

def is_smb_mounted(smb_server, smb_path, mount_point):
    try:
        result = subprocess.run(['mount'], stdout=subprocess.PIPE, text=True)
        output = result.stdout
        
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
        cmd = ['sudo', 'mount_smbfs', f"//GUEST@{smb_server}{smb_path}", mount_point]
        logging.info(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        logging.info(f"Mounted //GUEST@{smb_server}{smb_path} on {mount_point}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error mounting SMB (exit status {e.returncode}): {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return False

def ensure_smb_mounted(smb_server, smb_path, mount_point):
    for attempt in range(MAX_RETRIES):
        if is_smb_mounted(smb_server, smb_path, mount_point):
            return True
        logging.info(f"//GUEST@{smb_server}{smb_path} is not mounted. Mounting now... (Attempt {attempt + 1}/{MAX_RETRIES})")
        if mount_smb(smb_server, smb_path, mount_point):
            return True
        time.sleep(RETRY_DELAY)
    logging.error(f"Failed to mount SMB after {MAX_RETRIES} attempts")
    return False

def retry_on_smb_failure(func):
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except OSError as e:
                if attempt < MAX_RETRIES - 1:
                    logging.warning(f"SMB operation failed: {e}. Attempting to remount... (Attempt {attempt + 1}/{MAX_RETRIES})")
                    if ensure_smb_mounted(SMB_SERVER, SMB_PATH, MOUNT_POINT):
                        continue
                logging.error(f"SMB operation failed after {MAX_RETRIES} attempts: {e}")
                raise
    return wrapper

@retry_on_smb_failure
def scan_files(scan_path, db_conn):
    logging.info(f"Scanning for new files in {scan_path}")
    cursor = db_conn.cursor()
    files_processed = 0
    start_time = int(time.time())

    # Get the timestamp of the last full scan
    cursor.execute("SELECT value FROM metadata WHERE key = 'last_full_scan'")
    result = cursor.fetchone()
    last_full_scan = int(result[0]) if result else 0

    for root, _, files in os.walk(scan_path):
        for file in files:
            if any(file.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
                try:
                    file_path = os.path.join(root, file)
                    last_modified = int(os.path.getmtime(file_path))
                    
                    # Check if the file was modified after the last full scan
                    if last_modified > last_full_scan:
                        # Check if the file already exists in the database
                        cursor.execute("SELECT last_modified FROM media_files WHERE file_path = ?", (file_path,))
                        result = cursor.fetchone()
                        
                        if not result or last_modified > result[0]:
                            # File is new or has been modified, update it
                            file_size = os.path.getsize(file_path)
                            content_type = "movie" if "movies" in root.lower() else "tv_show"
                            audio_tracks, subtitle_tracks = get_file_metadata(file_path)
                            
                            cursor.execute("""INSERT OR REPLACE INTO media_files 
                                              (file_path, file_basename, file_size, last_modified, 
                                               content_type, audio_metadata, subtitle_metadata, needs_compression, has_been_reviewed) 
                                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                           (file_path, os.path.basename(file_path), file_size, last_modified,
                                            content_type, json.dumps(audio_tracks), json.dumps(subtitle_tracks), None, None))
                            
                            logging.info(f"Added/Updated file: {file_path}")
                            files_processed += 1
                    
                    if files_processed % DB_WRITE_INTERVAL == 0:
                        db_conn.commit()
                        logging.info(f"Committed {files_processed} files to database")
                    
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")

    # Update the last full scan timestamp
    cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", ("last_full_scan", str(start_time)))
    
    db_conn.commit()  # Final commit for any remaining files
    logging.info(f"Scan completed. Total files processed: {files_processed}")

def open_in_vlc(file_path):
    system = platform.system()
    
    if system == "Darwin":  # macOS
        vlc_command = "/Applications/VLC.app/Contents/MacOS/VLC"
    elif system == "Windows":
        vlc_command = r"C:\Program Files\VideoLAN\VLC\vlc.exe"
    else:  # Linux and other Unix-like systems
        vlc_command = "vlc"

    try:
        # Use shlex.quote to properly escape the file path
        quoted_path = shlex.quote(file_path)
        full_command = f"{vlc_command} {quoted_path}"
        
        logging.info(f"Attempting to open VLC with command: {full_command}")
        
        process = subprocess.Popen(
            full_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for a short time to see if the process immediately exits
        try:
            process.wait(timeout=5)
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                logging.error(f"VLC process exited with code {process.returncode}")
                logging.error(f"stdout: {stdout.decode('utf-8', errors='replace')}")
                logging.error(f"stderr: {stderr.decode('utf-8', errors='replace')}")
                return False
        except subprocess.TimeoutExpired:
            # Process is still running, which is good
            logging.info("VLC process started successfully")
            return True
        
        return True
    except Exception as e:
        logging.error(f"Error opening file in VLC: {e}")
        return False

def delete_file(file_path, db_conn):

    rv = True
    try:
        os.remove(file_path)
        logging.info(f"Deleted file: {file_path}")
    except Exception as e:
        logging.error(f"Error deleting file {file_path}: {e}")
        rv = False
        
    try:
        cursor = db_conn.cursor()
        cursor.execute("DELETE FROM media_files WHERE file_path = ?", (file_path,))
        db_conn.commit()
        logging.info(f"Dropping file: {file_path} from database")
    except Exception as e:
        logging.error(f"Error deleting row from database {file_path}: {e}")
        rv = False

    return rv


def is_english_language(language):
    if not language:
        return False
    language = language.lower()
    english_indicators = [
        'english', 'eng', 'en',  # Common names and abbreviations
        'en-us', 'en-gb', 'en-au', 'en-ca',  # Country-specific variants
        'eng-us', 'eng-gb', 'eng-au', 'eng-ca',
        'en-uk', 'eng-uk',  # Additional UK variants
        'en-nz', 'eng-nz',  # New Zealand
        'en-ie', 'eng-ie',  # Ireland
        'en-za', 'eng-za',  # South Africa
        'en-in', 'eng-in',  # India
        'en-sg', 'eng-sg',  # Singapore
        'eng dum', 'eng dub',  # Common misspellings or abbreviations
        'english dub', 'english dubbed'
    ]
    return any(indicator in language or language.startswith(indicator) for indicator in english_indicators)

def update_language_metadata(file_path, stream_type, stream_index, language):
    try:
        # Construct the FFmpeg command
        cmd = [
            'ffmpeg',
            '-i', file_path,
            '-map', '0',
            '-c', 'copy',
            f'-metadata:s:{stream_type}:{stream_index}', f'language={language}',
            f'{file_path}.temp'
        ]
        
        # Run the FFmpeg command
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        # Replace the original file with the new one
        os.replace(f'{file_path}.temp', file_path)
        
        logging.info(f"Updated {stream_type} stream {stream_index} language to {language} for {file_path}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error updating language metadata: {e}")
        logging.error(f"FFmpeg stderr: {e.stderr}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error updating language metadata: {e}")
        return False


def get_files_without_english_audio(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT file_path, audio_metadata, subtitle_metadata
        FROM media_files
        WHERE has_been_reviewed IS NULL OR has_been_reviewed = 0
    """)
    
    files_to_review = []
    for file_path, audio_metadata, subtitle_metadata in cursor.fetchall():
        try:
            audio_tracks = json.loads(audio_metadata)
            has_english_audio = any(is_english_language(track.get('language', '')) for track in audio_tracks)
            if not has_english_audio:
                files_to_review.append((file_path, audio_metadata, subtitle_metadata))
        except json.JSONDecodeError:
            logging.warning(f"Invalid JSON in audio_metadata for {file_path}")
            files_to_review.append((file_path, audio_metadata, subtitle_metadata))
    
    return files_to_review

def review_files(db_conn):
    files_to_review = get_files_without_english_audio(db_conn)
    
    for file_path, audio_metadata, subtitle_metadata in files_to_review:
        print(f"\nReviewing file: {file_path}")
        
        # Print file metadata
        print("Audio tracks:")
        try:
            audio_tracks = json.loads(audio_metadata)
            for i, track in enumerate(audio_tracks):
                print(f"  {i}: Language: {track.get('language', 'Unknown')}, Format: {track.get('format', 'Unknown')}")
        except json.JSONDecodeError:
            print("  Unable to parse audio metadata")
        
        print("English subtitle tracks:")
        try:
            subtitle_tracks = json.loads(subtitle_metadata)
            english_subtitles = [
                (i, track) for i, track in enumerate(subtitle_tracks)
                if is_english_language(track.get('language', ''))
            ]
            if english_subtitles:
                for i, track in english_subtitles:
                    print(f"  {i}: Format: {track.get('format', 'Unknown')}")
            else:
                print("  No English subtitles found")
        except json.JSONDecodeError:
            print("  Unable to parse subtitle metadata")
        
        # Ask if user wants to open in VLC
        while True:
            choice = input("Do you want to open this file in VLC? (y/n): ").lower()
            if choice in ['y', 'n']:
                break
            print("Invalid input. Please enter 'y' or 'n'.")
        
        if choice == 'y':
            if ensure_smb_mounted(SMB_SERVER, SMB_PATH, MOUNT_POINT):            
                open_in_vlc(file_path)
                input("Press Enter when you're done reviewing the file in VLC...")
            else:
                break
        
        # Ask if user wants to keep the file
        while True:
            keep_file = input("Do you want to keep this file? (y/n): ").lower()
            if keep_file in ['y', 'n']:
                break
            print("Invalid input. Please enter 'y' or 'n'.")
        
        if keep_file == 'y':
            # Update status in database
            cursor = db_conn.cursor()
            cursor.execute("UPDATE media_files SET has_been_reviewed = 1 WHERE file_path = ?", (file_path,))
            db_conn.commit()
            print(f"File kept and marked as reviewed: {file_path}")
        else:
            # Delete the file
            if delete_file(file_path, db_conn):
                print(f"File deleted: {file_path}")
            else:
                print(f"Failed to delete file: {file_path}")
        
        print("\n--- End of file review ---\n")


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

def compress_file(file_path, db_conn):
    try:
        logging.info(f"Beginning to compress {os.path.basename(file_path)}")

        # Copy the file to local directory
        input_file = os.path.join(DESTINATION_DIR, os.path.basename(file_path))
        
        # Replace extension with .mp4
        base, ext = os.path.splitext(input_file)
        if ext.lower() == '.mp4':
            output_file = base + '.mp42'
            logging.info(f"Destination path {input_file} already ends with .mp4. Using .mp42 for temporary output.")
        else:
            output_file = base + '.mp4'

        logging.info(f"Copying {os.path.basename(file_path)}")
        ensure_smb_mounted(SMB_SERVER, SMB_PATH, MOUNT_POINT)
        if not copy_with_retries(file_path, input_file):
            raise Exception(f"Failed to copy {file_path}")

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

        # Rename .mp42 back to .mp4 if necessary
        if output_file.endswith('.mp42'):
            final_output_file = output_file.replace('.mp42', '.mp4')
            os.rename(output_file, final_output_file)
            output_file = final_output_file

        # Copy the output file to the original file directory and remove the original file
        logging.info(f"Compression of {os.path.basename(file_path)} completed, copying {output_file} to original directory")
        ensure_smb_mounted(SMB_SERVER, SMB_PATH, MOUNT_POINT)
        if not copy_with_retries(output_file, os.path.join(os.path.dirname(file_path), os.path.basename(output_file))):
            raise Exception(f"Failed to copy {output_file}")

        # Remove the output file
        logging.info(f"Unlinking {output_file}")
        os.remove(output_file)

        # Remove the original file
        logging.info(f"Unlinking {file_path}")
        ensure_smb_mounted(SMB_SERVER, SMB_PATH, MOUNT_POINT)
        os.remove(file_path)

        # Update the database with new file size and metadata
        new_file_path = os.path.join(os.path.dirname(file_path), os.path.basename(output_file))
        new_file_size = os.path.getsize(new_file_path)
        new_audio_tracks, new_subtitle_tracks = get_file_metadata(new_file_path)

        cursor = db_conn.cursor()
        cursor.execute("""
            UPDATE media_files 
            SET file_path = ?, file_size = ?, last_modified = ?, 
                audio_metadata = ?, subtitle_metadata = ?, needs_compression = 0
            WHERE file_path = ?
        """, (new_file_path, new_file_size, int(time.time()), 
              json.dumps(new_audio_tracks), json.dumps(new_subtitle_tracks), file_path))
        db_conn.commit()

        logging.info(f"Successfully compressed and updated database for {file_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to compress file {file_path}: {e}")
        return False

def compress_files(file_type, size_threshold, db_conn):
    cursor = db_conn.cursor()
    
    # Select files of the specified type that need compression
    query = """
        SELECT file_path 
        FROM media_files 
        WHERE content_type = ? AND file_size > ? AND (needs_compression IS NULL OR needs_compression = 1)
        ORDER BY file_size DESC
    """
    cursor.execute(query, (file_type, size_threshold))
    
    files_to_compress = cursor.fetchall()
    
    for (file_path,) in files_to_compress:
        if compress_file(file_path, db_conn):
            logging.info(f"Successfully compressed {file_path}")
        else:
            logging.error(f"Failed to compress {file_path}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py [scan <directory> | review [count] | compress <type> <size_threshold>]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "scan" and len(sys.argv) == 3:
        scan_directory = sys.argv[2]
        if not os.path.isdir(scan_directory):
            print(f"Error: {scan_directory} is not a valid directory")
            sys.exit(1)

        if not ensure_smb_mounted(SMB_SERVER, SMB_PATH, MOUNT_POINT):
            print("Failed to mount SMB share. Exiting.")
            sys.exit(1)

        conn = create_db_connection(DB_PATH)
        try:
            scan_files(scan_directory, conn)
        finally:
            conn.close()
    elif command == "review":
        conn = create_db_connection(DB_PATH)
        try:
            if len(sys.argv) == 3 and sys.argv[2] == "count":
                count = get_files_without_english_audio(conn)
                print(f"Number of files to review: {len(count)}")
            else:
                review_files(conn)
        finally:
            conn.close()
    elif command == "compress" and len(sys.argv) == 4:
        file_type = sys.argv[2]
        size_threshold = int(sys.argv[3])
        
        if file_type not in ["movie", "tv_show"]:
            print("Error: file type must be either 'movie' or 'tv_show'")
            sys.exit(1)
        
        conn = create_db_connection(DB_PATH)
        try:
            compress_files(file_type, size_threshold, conn)
        finally:
            conn.close()
    else:
        print("Usage: python script.py [scan <directory> | review [count] | compress <type> <size_threshold>]")
        sys.exit(1)

if __name__ == "__main__":
    main()