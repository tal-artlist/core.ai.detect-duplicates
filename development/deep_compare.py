import os
import sys
import json
import hashlib
import subprocess
import numpy as np
from typing import Dict, Any

# Try to import librosa
try:
    import librosa
    HAS_LIBROSA = True
except ImportError:
    HAS_LIBROSA = False
    print("⚠️  librosa not found.")

def get_file_hash(filepath: str) -> str:
    """Calculate MD5 hash of the entire file"""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def get_audio_content_hash(filepath: str) -> str:
    """
    Decode audio to raw PCM samples and calculate SHA256 hash.
    This ignores metadata, headers, and container differences.
    """
    if not HAS_LIBROSA:
        return "N/A (librosa missing)"
    
    try:
        # Load audio as mono, native sample rate
        y, sr = librosa.load(filepath, sr=None, mono=True)
        # Convert to bytes (float32 array)
        audio_bytes = y.tobytes()
        return hashlib.sha256(audio_bytes).hexdigest()
    except Exception as e:
        return f"Error: {e}"

def get_metadata(filepath: str) -> Dict[str, Any]:
    """Extract metadata using ffprobe"""
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        filepath
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            return {"error": "ffprobe failed"}
        
        data = json.loads(result.stdout)
        format_data = data.get("format", {})
        tags = format_data.get("tags", {})
        
        return {
            "duration": format_data.get("duration"),
            "size": format_data.get("size"),
            "bit_rate": format_data.get("bit_rate"),
            "format_name": format_data.get("format_name"),
            "tags": tags
        }
    except Exception as e:
        return {"error": str(e)}

def deep_compare(file1: str, file2: str):
    """Perform deep comparison of two files"""
    print(f"🔍 Deep Comparing:\n  A: {os.path.basename(file1)}\n  B: {os.path.basename(file2)}\n")
    
    # 1. File Hash
    hash1 = get_file_hash(file1)
    hash2 = get_file_hash(file2)
    print(f"1️⃣  File MD5 Hash (Container + Content):")
    print(f"   A: {hash1}")
    print(f"   B: {hash2}")
    if hash1 == hash2:
        print("   ✅ EXACT MATCH (Files are identical bytes)")
    else:
        print("   ❌ DIFFERENT (Files differ in bytes)")
    print("-" * 40)

    # 2. Audio Content Hash
    print(f"2️⃣  Audio Content SHA256 (Decoded Audio Only):")
    audio_hash1 = get_audio_content_hash(file1)
    audio_hash2 = get_audio_content_hash(file2)
    print(f"   A: {audio_hash1}")
    print(f"   B: {audio_hash2}")
    
    if audio_hash1 == audio_hash2 and "Error" not in audio_hash1:
        print("   ✅ EXACT MATCH (The sound is 100% identical)")
        print("      👉 This proves the audio payload is the same, regardless of metadata.")
    else:
        print("   ❌ DIFFERENT (The sound is different)")
    print("-" * 40)

    # 3. Metadata Comparison
    print(f"3️⃣  Metadata/Tags (ffprobe):")
    meta1 = get_metadata(file1)
    meta2 = get_metadata(file2)
    
    # Find differences in tags
    tags1 = meta1.get("tags", {})
    tags2 = meta2.get("tags", {})
    
    all_keys = set(tags1.keys()) | set(tags2.keys())
    diffs = []
    for key in all_keys:
        val1 = tags1.get(key, "N/A")
        val2 = tags2.get(key, "N/A")
        if val1 != val2:
            diffs.append((key, val1, val2))
            
    if diffs:
        print("   ⚠️  Metadata Differences Found:")
        for key, v1, v2 in diffs:
            print(f"      - {key}:")
            print(f"        A: {v1}")
            print(f"        B: {v2}")
    else:
        print("   ✅ No Metadata Differences Found")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python deep_compare.py <file1> <file2>")
        sys.exit(1)
        
    file1 = sys.argv[1]
    file2 = sys.argv[2]
    
    if not os.path.exists(file1) or not os.path.exists(file2):
        print("❌ One or both files do not exist")
        sys.exit(1)
        
    deep_compare(file1, file2)
