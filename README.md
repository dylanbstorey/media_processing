# Media Manager Script

This Python script helps manage a media library by scanning, reviewing, and compressing video files. It uses a SQLite database to track file information and supports SMB file sharing.

## Database Schema

The script uses a SQLite database with the following tables:

1. `media_files`:
   - `file_path` (TEXT, PRIMARY KEY): Full path to the media file
   - `file_basename` (TEXT): Base name of the file
   - `file_size` (INTEGER): Size of the file in bytes
   - `last_modified` (INTEGER): Last modification timestamp
   - `content_type` (TEXT): Type of content ('movie' or 'tv_show')
   - `audio_metadata` (TEXT): JSON string containing audio track information
   - `subtitle_metadata` (TEXT): JSON string containing subtitle track information
   - `needs_compression` (BOOLEAN): Flag indicating if the file needs compression
   - `has_been_reviewed` (BOOLEAN): Flag indicating if the file has been reviewed

2. `metadata`:
   - `key` (TEXT, PRIMARY KEY): Metadata key
   - `value` (TEXT): Metadata value

## Usage Instructions

The script supports the following commands:

1. Scan files:
   ```
   python media_manager.py scan <directory>
   ```

2. Review files:
   ```
   python media_manager.py review
   ```

3. Count files needing review:
   ```
   python media_manager.py review count
   ```

4. Compress files:
   ```
   python media_manager.py compress <type> <size_threshold>
   ```
   Where `<type>` is either "movie" or "tv_show", and `<size_threshold>` is the minimum file size in bytes for compression.

## Installation and Required Tools

1. Python 3.6 or higher

2. Required Python packages:
   ```
   pip install sqlite3
   ```

3. HandBrakeCLI:
   - macOS: `brew install handbrake`
   - Linux: Follow instructions at https://handbrake.fr/downloads.php
   - Windows: Download from https://handbrake.fr/downloads.php

4. MediaInfo:
   - macOS: `brew install mediainfo`
   - Linux: `sudo apt-get install mediainfo` (Ubuntu/Debian) or `sudo yum install mediainfo` (CentOS/Fedora)
   - Windows: Download from https://mediaarea.net/en/MediaInfo/Download/Windows

5. VLC Media Player:
   - Download and install from https://www.videolan.org/vlc/

6. SMB File Sharing:
   - Ensure your system supports SMB file sharing (built-in for most modern operating systems)

7. Configure the script:
   - Update the following constants in the script to match your environment:
     - `DB_PATH`
     - `SMB_SERVER`
     - `SMB_PATH`
     - `MOUNT_POINT`
     - `DESTINATION_DIR`
     - `HANDBRAKE_PRESET`

Note: Ensure you have the necessary permissions to mount SMB shares and modify files in the specified directories.