#!/usr/bin/env python3

import subprocess
import os
import sys
import json

VIDEO_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.webm', '.m4v', 
    '.mpg', '.mpeg', '.3gp', '.3g2', '.ogv', '.vob', '.rmvb', '.asf',
    '.m2ts', '.mts', '.ts', '.divx', '.f4v'
}

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
                    return None  # English audio found, no output needed
        
        if has_audio:
            return file_path  # Non-English audio found
        else:
            return None  # No audio found, no output needed
    except subprocess.CalledProcessError:
        return f"Error: mediainfo failed for {file_path}"
    except Exception as e:
        return f"Error processing {file_path}: {str(e)}"

def main(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            if any(file.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
                file_path = os.path.join(root, file)
                result = check_non_english_audio(file_path)
                if result:
                    print(result)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory>")
        sys.exit(1)
    
    directory = sys.argv[1]
    if not os.path.isdir(directory):
        print(f"Error: {directory} is not a valid directory")
        sys.exit(1)
    
    main(directory)