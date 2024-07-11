# Media Processing Scripts


Just a collection of scripts I run to help me organize, optimize, and validate my media collection(s). Used in conjunction with 
other applications from the servarr stack that I use to identify and grab media for collection. 


# Index 

- `compress_files.py` - scans a directory for files greater than a specific threshold then runs them through handbrake.
    - compress.json - a Handbrake profile for compression.
- `english_audio.py` - scans a director for files that don't appear to have an english language audio track. (requires media info)
