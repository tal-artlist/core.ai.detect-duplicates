#!/usr/bin/env python3
"""
Test script to compare the same audio content in different formats (WAV vs MP3)
"""

import os
import sys
import ctypes
from pathlib import Path

def setup_chromaprint():
    """Set up Chromaprint library and environment"""
    os.environ['DYLD_LIBRARY_PATH'] = '/opt/homebrew/lib:' + os.environ.get('DYLD_LIBRARY_PATH', '')
    
    try:
        chromaprint_lib = ctypes.CDLL('/opt/homebrew/lib/libchromaprint.dylib')
        import acoustid
        
        fpcalc_path = "/opt/homebrew/bin/fpcalc"
        os.environ['FPCALC_COMMAND'] = fpcalc_path
        acoustid.FPCALC_COMMAND = fpcalc_path
        
        print(f"✅ Setup complete - acoustid.have_chromaprint: {acoustid.have_chromaprint}")
        return acoustid
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return None

def analyze_file(acoustid, file_path):
    """Analyze a single file and return detailed info"""
    try:
        duration, fingerprint = acoustid.fingerprint_file(str(file_path))
        return {
            'file': file_path.name,
            'path': str(file_path),
            'exists': file_path.exists(),
            'size_mb': file_path.stat().st_size / (1024*1024) if file_path.exists() else 0,
            'format': file_path.suffix.upper(),
            'duration': duration,
            'fingerprint': fingerprint,
            'fingerprint_length': len(fingerprint) if fingerprint else 0,
            'success': True
        }
    except Exception as e:
        return {
            'file': file_path.name,
            'path': str(file_path),
            'exists': file_path.exists(),
            'error': str(e),
            'success': False
        }

def main():
    print("=== Format Comparison Test: WAV vs MP3 ===\n")
    
    acoustid = setup_chromaprint()
    if not acoustid:
        return 1
    
    # Test files
    wav_file = Path("/Users/tal.darchi/work/core.ai.detect-duplicates/samples/artlist/10001_74258_74258_Soft_In_the_Head_-16-44.1-.wav")
    mp3_file = Path("/Users/tal.darchi/work/core.ai.detect-duplicates/samples/artlist/10001_74259_74259_Soft_In_the_Head_-16-44.1-.mp3")
    
    print("🎵 Analyzing files...")
    
    # Analyze both files
    wav_info = analyze_file(acoustid, wav_file)
    mp3_info = analyze_file(acoustid, mp3_file)
    
    # Display file information
    print(f"\n📁 File Information:")
    print(f"{'Property':<20} {'WAV File':<40} {'MP3 File':<40}")
    print("-" * 100)
    print(f"{'Exists':<20} {wav_info['exists']:<40} {mp3_info['exists']:<40}")
    
    if wav_info['exists'] and mp3_info['exists']:
        print(f"{'Size (MB)':<20} {wav_info['size_mb']:<40.1f} {mp3_info['size_mb']:<40.1f}")
        
        if wav_info['success'] and mp3_info['success']:
            print(f"{'Duration (s)':<20} {wav_info['duration']:<40.1f} {mp3_info['duration']:<40.1f}")
            print(f"{'Fingerprint Length':<20} {wav_info['fingerprint_length']:<40} {mp3_info['fingerprint_length']:<40}")
            
            # Compare fingerprints
            print(f"\n🔍 Fingerprint Comparison:")
            try:
                wav_tuple = (wav_info['duration'], wav_info['fingerprint'])
                mp3_tuple = (mp3_info['duration'], mp3_info['fingerprint'])
                similarity = acoustid.compare_fingerprints(wav_tuple, mp3_tuple)
                
                print(f"Similarity Score: {similarity:.6f}")
                
                # Interpret the result
                if similarity >= 0.95:
                    status = "🎯 NEARLY IDENTICAL - Same audio content in different formats"
                elif similarity >= 0.80:
                    status = "🔍 VERY SIMILAR - Likely same content with format differences"
                elif similarity >= 0.50:
                    status = "📊 SIMILAR - Related content but noticeable differences"
                elif similarity >= 0.10:
                    status = "📈 SOMEWHAT SIMILAR - Some common elements"
                else:
                    status = "❌ DIFFERENT - Completely different audio content"
                
                print(f"Status: {status}")
                
                # Additional analysis
                duration_diff = abs(wav_info['duration'] - mp3_info['duration'])
                size_ratio = wav_info['size_mb'] / mp3_info['size_mb'] if mp3_info['size_mb'] > 0 else 0
                
                print(f"\n📊 Additional Analysis:")
                print(f"Duration difference: {duration_diff:.2f} seconds")
                print(f"Size ratio (WAV/MP3): {size_ratio:.1f}x")
                print(f"Fingerprint length difference: {abs(wav_info['fingerprint_length'] - mp3_info['fingerprint_length'])} characters")
                
                # Show fingerprint previews
                print(f"\n🔬 Fingerprint Previews:")
                print(f"WAV: {str(wav_info['fingerprint'])[:100]}...")
                print(f"MP3: {str(mp3_info['fingerprint'])[:100]}...")
                
                return 0
                
            except Exception as e:
                print(f"❌ Comparison failed: {e}")
                return 1
        else:
            print(f"\n❌ Processing errors:")
            if not wav_info['success']:
                print(f"WAV error: {wav_info.get('error', 'Unknown error')}")
            if not mp3_info['success']:
                print(f"MP3 error: {mp3_info.get('error', 'Unknown error')}")
            return 1
    else:
        print(f"\n❌ File availability:")
        if not wav_info['exists']:
            print(f"WAV file not found: {wav_file}")
        if not mp3_info['exists']:
            print(f"MP3 file not found: {mp3_file}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
