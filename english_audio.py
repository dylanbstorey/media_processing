#!/usr/bin/env python3

import subprocess
import os
import sys
import json
import sqlite3
import hashlib

VIDEO_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.webm', '.m4v', 
    '.mpg', '.mpeg', '.3gp', '.3g2', '.ogv', '.vob', '.rmvb', '.asf',
    '.m2ts', '.mts', '.ts', '.divx', '.f4v'
}

DB_FILE = "files.db"

def create_db_connection(db_file):
    conn = sqlite3.connect(db_file)
    conn.execute('''CREATE TABLE IF NOT EXISTS english_audio
                    (filename TEXT PRIMARY KEY, 
                     file_path TEXT, 
                     file_basename TEXT, 
                     last_modified INTEGER, 
                     no_english INTEGER)''')
    return conn

def file_hash(filename):
    return hashlib.md5(filename.encode()).hexdigest()

def check_non_english_audio(file_path):
    try:
        result = subprocess.run([
            'mediainfo',
            '--Output=JSON',
            file_path
        ], capture_output=True, text=True, check=True)

        data = json.loads(result.stdout)
        
        has_audio = False
        for track in data.get('media', {}).get('track', []):
            if track.get('@type') == 'Audio':
                has_audio = True
                language = track.get('Language', '').lower()
                if language == '' or language == 'en' or language == 'eng':
                    return 0  # English audio found
        
        if has_audio:
            return 1  # Non-English audio found
        else:
            return None  # No audio found
    except subprocess.CalledProcessError:
        print(f"Error: mediainfo failed for {file_path}")
        return None
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return None

def main(directory, db_conn):
    cursor = db_conn.cursor()
    for root, _, files in os.walk(directory):
        for file in files:
            if any(file.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
                file_path = os.path.join(root, file)
                file_key = file_hash(file_path)
                file_basename = os.path.basename(file_path)
                last_modified = os.path.getmtime(file_path)

                cursor.execute("SELECT last_modified, no_english FROM english_audio WHERE filename = ?", (file_key,))
                result = cursor.fetchone()

                if result and result[0] == last_modified and result[1] is not None:
                    continue  # Skip this file as it has been processed before and hasn't changed

                no_english = check_non_english_audio(file_path)

                if no_english == 1:
                    print(f"No English audio detected: {file_path}")

                cursor.execute("""REPLACE INTO english_audio 
                                  (filename, file_path, file_basename, last_modified, no_english) 
                                  VALUES (?, ?, ?, ?, ?)""",
                               (file_key, file_path, file_basename, last_modified, no_english))
                db_conn.commit()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory>")
        sys.exit(1)
    
    directory = sys.argv[1]
    if not os.path.isdir(directory):
        print(f"Error: {directory} is not a valid directory")
        sys.exit(1)
    
    conn = create_db_connection(DB_FILE)
    try:
        main(directory, conn)
    finally:
        conn.close()