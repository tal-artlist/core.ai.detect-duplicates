#!/usr/bin/env python3
"""
Test script to demonstrate duplicate detection with known duplicates
"""

import os
import sys
import ctypes
from pathlib import Path
import time

def setup_chromaprint():
    """Set up Chromaprint library and environment"""
    os.environ['DYLD_LIBRARY_PATH'] = '/opt/homebrew/lib:' + os.environ.get('DYLD_LIBRARY_PATH', '')
    
    try:
        chromaprint_lib = ctypes.CDLL('/opt/homebrew/lib/libchromaprint.dylib')
        import acoustid
        
        fpcalc_path = "/opt/homebrew/bin/fpcalc"
        os.environ['FPCALC_COMMAND'] = fpcalc_path
        acoustid.FPCALC_COMMAND = fpcalc_path
        
        return acoustid
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return None

def main():
    print("=== Duplicate Detection Test ===\n")
    
    acoustid = setup_chromaprint()
    if not acoustid:
        return 1
    
    # Test files: originals and their duplicates
    test_files = [
        Path("samples/artlist/733727_724629_Heavy_Rain_Drop-_Rainfall_on_Ground_.mp3"),
        Path("test_duplicate1.mp3"),  # duplicate of above
        Path("samples/artlist/735301_Nobou_-_Hope_And_Glory_-_Master_-_84_BPM_-_300822_-_EXT_-_2444.mp3"),
        Path("test_duplicate2.mp3"),  # duplicate of above
        Path("samples/motionarray/Sadness_Ambient_original.mp3"),  # different file
    ]
    
    # Generate fingerprints
    fingerprints = {}
    for file_path in test_files:
        if file_path.exists():
            print(f"Processing: {file_path.name}")
            duration, fp = acoustid.fingerprint_file(str(file_path))
            fingerprints[file_path.name] = (duration, fp)
        else:
            print(f"⚠️  File not found: {file_path}")
    
    print(f"\n🔍 Comparing all pairs:")
    print(f"{'File 1':<35} {'File 2':<35} {'Similarity':<12} {'Status'}")
    print("-" * 95)
    
    files = list(fingerprints.keys())
    for i in range(len(files)):
        for j in range(i + 1, len(files)):
            file1, file2 = files[i], files[j]
            
            try:
                score = acoustid.compare_fingerprints(fingerprints[file1], fingerprints[file2])
                
                # Determine status
                if score >= 0.99:
                    status = "🎯 IDENTICAL"
                elif score >= 0.8:
                    status = "🔍 VERY SIMILAR"
                elif score >= 0.5:
                    status = "📊 SIMILAR"
                elif score >= 0.1:
                    status = "📈 SOMEWHAT SIMILAR"
                else:
                    status = "❌ DIFFERENT"
                
                print(f"{file1[:34]:<35} {file2[:34]:<35} {score:<12.6f} {status}")
                
            except Exception as e:
                print(f"{file1[:34]:<35} {file2[:34]:<35} {'ERROR':<12} ❌ {e}")
    
    print(f"\n💡 Interpretation:")
    print(f"   1.000000 = Identical files (perfect duplicates)")
    print(f"   0.800000+ = Very high similarity (likely duplicates)")
    print(f"   0.500000+ = Moderate similarity (similar content)")
    print(f"   0.100000+ = Low similarity (some common elements)")
    print(f"   0.000000 = No similarity (completely different)")

if __name__ == "__main__":
    sys.exit(main())
