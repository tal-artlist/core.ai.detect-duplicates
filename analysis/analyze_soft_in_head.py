#!/usr/bin/env python3
"""
Deep analysis of the "Soft In the Head" files to understand why similarity is ~0.8
instead of the expected 0.99+ for format conversion
"""

import os
import sys
import subprocess

# Auto-restart with correct environment if needed
if 'DYLD_LIBRARY_PATH' not in os.environ or '/opt/homebrew/lib' not in os.environ.get('DYLD_LIBRARY_PATH', ''):
    env = os.environ.copy()
    env['DYLD_LIBRARY_PATH'] = '/opt/homebrew/lib:' + env.get('DYLD_LIBRARY_PATH', '')
    result = subprocess.run([sys.executable] + sys.argv, env=env)
    sys.exit(result.returncode)

import ctypes
from pathlib import Path
import json
import pandas as pd

def setup_chromaprint():
    """Set up Chromaprint library and environment"""
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

def get_detailed_audio_info(file_path):
    """Get comprehensive audio file information using ffprobe"""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json', 
            '-show_format', '-show_streams', '-show_chapters',
            str(file_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        format_info = data.get('format', {})
        audio_stream = next((s for s in data.get('streams', []) if s['codec_type'] == 'audio'), {})
        
        return {
            'filename': format_info.get('filename', ''),
            'format_name': format_info.get('format_name', ''),
            'format_long_name': format_info.get('format_long_name', ''),
            'duration': float(format_info.get('duration', 0)),
            'size': int(format_info.get('size', 0)),
            'bitrate': int(format_info.get('bit_rate', 0)),
            'codec_name': audio_stream.get('codec_name', ''),
            'codec_long_name': audio_stream.get('codec_long_name', ''),
            'sample_rate': int(audio_stream.get('sample_rate', 0)),
            'channels': int(audio_stream.get('channels', 0)),
            'channel_layout': audio_stream.get('channel_layout', ''),
            'bits_per_sample': audio_stream.get('bits_per_sample', 0),
            'r_frame_rate': audio_stream.get('r_frame_rate', ''),
            'avg_frame_rate': audio_stream.get('avg_frame_rate', ''),
            'tags': audio_stream.get('tags', {}),
            'format_tags': format_info.get('tags', {})
        }
    except Exception as e:
        print(f"Error getting audio info for {file_path}: {e}")
        return {}

def get_raw_fingerprint_data(file_path):
    """Get raw fingerprint data using fpcalc directly"""
    try:
        fpcalc_path = "/opt/homebrew/bin/fpcalc"
        
        # Get raw fingerprint
        cmd = [fpcalc_path, "-json", "-raw", str(file_path)]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        raw_data = json.loads(result.stdout)
        
        # Get compressed fingerprint
        cmd = [fpcalc_path, "-json", str(file_path)]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        compressed_data = json.loads(result.stdout)
        
        return {
            'duration': raw_data.get('duration', 0),
            'raw_fingerprint': raw_data.get('fingerprint', []),
            'compressed_fingerprint': compressed_data.get('fingerprint', ''),
            'raw_fp_length': len(raw_data.get('fingerprint', [])),
            'compressed_fp_length': len(compressed_data.get('fingerprint', ''))
        }
    except Exception as e:
        print(f"Error getting fingerprint data for {file_path}: {e}")
        return {}

def analyze_fingerprint_differences(fp1_raw, fp2_raw):
    """Analyze differences between raw fingerprints"""
    if not fp1_raw or not fp2_raw:
        return {}
    
    min_len = min(len(fp1_raw), len(fp2_raw))
    max_len = max(len(fp1_raw), len(fp2_raw))
    
    if min_len == 0:
        return {'error': 'Empty fingerprints'}
    
    # Compare overlapping portion
    differences = []
    matching_values = 0
    total_bits_compared = 0
    
    for i in range(min_len):
        val1, val2 = fp1_raw[i], fp2_raw[i]
        
        if val1 == val2:
            matching_values += 1
        else:
            # Count bit differences
            xor_result = val1 ^ val2
            bit_differences = bin(xor_result).count('1')
            differences.append({
                'position': i,
                'value1': val1,
                'value2': val2,
                'xor': xor_result,
                'bit_differences': bit_differences
            })
        
        # Count total bits (assuming 32-bit integers)
        total_bits_compared += 32
    
    # Calculate statistics
    matching_bits = sum(32 - d['bit_differences'] for d in differences) + (matching_values * 32)
    bit_similarity = matching_bits / total_bits_compared if total_bits_compared > 0 else 0
    
    return {
        'min_length': min_len,
        'max_length': max_len,
        'length_difference': max_len - min_len,
        'matching_values': matching_values,
        'different_values': len(differences),
        'value_match_ratio': matching_values / min_len,
        'bit_similarity': bit_similarity,
        'total_bit_differences': sum(d['bit_differences'] for d in differences),
        'avg_bit_differences_per_value': sum(d['bit_differences'] for d in differences) / len(differences) if differences else 0,
        'first_10_differences': differences[:10]
    }

def compare_audio_content(file1, file2):
    """Compare audio content at the waveform level"""
    try:
        # Extract a small sample from each file for comparison
        import tempfile
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract 10 seconds from the middle of each file as WAV
            sample1 = Path(temp_dir) / "sample1.wav"
            sample2 = Path(temp_dir) / "sample2.wav"
            
            # Get duration first
            info1 = get_detailed_audio_info(file1)
            info2 = get_detailed_audio_info(file2)
            
            mid_time1 = info1['duration'] / 2
            mid_time2 = info2['duration'] / 2
            
            # Extract 10-second samples from middle
            cmd1 = ['ffmpeg', '-y', '-i', str(file1), '-ss', str(mid_time1), '-t', '10', '-ar', '44100', '-ac', '2', str(sample1)]
            cmd2 = ['ffmpeg', '-y', '-i', str(file2), '-ss', str(mid_time2), '-t', '10', '-ar', '44100', '-ac', '2', str(sample2)]
            
            subprocess.run(cmd1, capture_output=True, check=True)
            subprocess.run(cmd2, capture_output=True, check=True)
            
            # Compare file sizes of normalized samples
            size1 = sample1.stat().st_size
            size2 = sample2.stat().st_size
            
            return {
                'sample1_size': size1,
                'sample2_size': size2,
                'size_ratio': size1 / size2 if size2 > 0 else 0,
                'size_difference_percent': abs(size1 - size2) / max(size1, size2) * 100
            }
            
    except Exception as e:
        return {'error': str(e)}

def main():
    print("=== Deep Analysis: Soft In the Head Files ===\n")
    
    acoustid = setup_chromaprint()
    if not acoustid:
        return 1
    
    # Files to analyze
    wav_file = Path("/Users/tal.darchi/work/core.ai.detect-duplicates/samples/artlist/10001_74258_74258_Soft_In_the_Head_-16-44.1-.wav")
    mp3_file = Path("/Users/tal.darchi/work/core.ai.detect-duplicates/samples/artlist/10001_74259_74259_Soft_In_the_Head_-16-44.1-.mp3")
    
    if not wav_file.exists() or not mp3_file.exists():
        print("❌ Files not found")
        return 1
    
    print(f"🎵 Analyzing files:")
    print(f"WAV: {wav_file.name}")
    print(f"MP3: {mp3_file.name}")
    
    # 1. Basic file information
    print(f"\n📁 FILE INFORMATION:")
    wav_info = get_detailed_audio_info(wav_file)
    mp3_info = get_detailed_audio_info(mp3_file)
    
    print(f"{'Property':<25} {'WAV':<30} {'MP3':<30} {'Difference'}")
    print("-" * 95)
    print(f"{'File Size (MB)':<25} {wav_file.stat().st_size/(1024*1024):<30.2f} {mp3_file.stat().st_size/(1024*1024):<30.2f} {(wav_file.stat().st_size - mp3_file.stat().st_size)/(1024*1024):+.2f}")
    print(f"{'Duration (s)':<25} {wav_info.get('duration', 0):<30.2f} {mp3_info.get('duration', 0):<30.2f} {wav_info.get('duration', 0) - mp3_info.get('duration', 0):+.2f}")
    print(f"{'Bitrate (bps)':<25} {wav_info.get('bitrate', 0):<30} {mp3_info.get('bitrate', 0):<30} {wav_info.get('bitrate', 0) - mp3_info.get('bitrate', 0):+}")
    print(f"{'Sample Rate (Hz)':<25} {wav_info.get('sample_rate', 0):<30} {mp3_info.get('sample_rate', 0):<30} {wav_info.get('sample_rate', 0) - mp3_info.get('sample_rate', 0):+}")
    print(f"{'Channels':<25} {wav_info.get('channels', 0):<30} {mp3_info.get('channels', 0):<30} {wav_info.get('channels', 0) - mp3_info.get('channels', 0):+}")
    print(f"{'Codec':<25} {wav_info.get('codec_name', 'N/A'):<30} {mp3_info.get('codec_name', 'N/A'):<30}")
    print(f"{'Bits per Sample':<25} {wav_info.get('bits_per_sample', 0):<30} {mp3_info.get('bits_per_sample', 0):<30}")
    
    # 2. Fingerprint analysis
    print(f"\n🔬 FINGERPRINT ANALYSIS:")
    
    # Get acoustid fingerprints
    wav_duration, wav_fp = acoustid.fingerprint_file(str(wav_file))
    mp3_duration, mp3_fp = acoustid.fingerprint_file(str(mp3_file))
    
    # Get raw fingerprint data
    wav_raw_data = get_raw_fingerprint_data(wav_file)
    mp3_raw_data = get_raw_fingerprint_data(mp3_file)
    
    print(f"{'Property':<25} {'WAV':<15} {'MP3':<15} {'Difference'}")
    print("-" * 65)
    print(f"{'Compressed FP Length':<25} {len(wav_fp):<15} {len(mp3_fp):<15} {len(wav_fp) - len(mp3_fp):+}")
    print(f"{'Raw FP Length':<25} {wav_raw_data.get('raw_fp_length', 0):<15} {mp3_raw_data.get('raw_fp_length', 0):<15} {wav_raw_data.get('raw_fp_length', 0) - mp3_raw_data.get('raw_fp_length', 0):+}")
    
    # 3. Similarity calculation
    similarity = acoustid.compare_fingerprints((wav_duration, wav_fp), (mp3_duration, mp3_fp))
    print(f"\n🎯 SIMILARITY SCORE: {similarity:.6f}")
    
    # 4. Raw fingerprint comparison
    print(f"\n🔍 RAW FINGERPRINT COMPARISON:")
    fp_analysis = analyze_fingerprint_differences(
        wav_raw_data.get('raw_fingerprint', []),
        mp3_raw_data.get('raw_fingerprint', [])
    )
    
    if 'error' not in fp_analysis:
        print(f"Fingerprint lengths: WAV={fp_analysis['max_length']}, MP3={fp_analysis['min_length']}")
        print(f"Length difference: {fp_analysis['length_difference']} values")
        print(f"Matching values: {fp_analysis['matching_values']}/{fp_analysis['min_length']} ({fp_analysis['value_match_ratio']:.1%})")
        print(f"Different values: {fp_analysis['different_values']}")
        print(f"Bit-level similarity: {fp_analysis['bit_similarity']:.6f}")
        print(f"Average bit differences per value: {fp_analysis['avg_bit_differences_per_value']:.1f}/32")
        
        if fp_analysis['first_10_differences']:
            print(f"\nFirst 10 fingerprint differences:")
            print(f"{'Pos':<4} {'WAV Value':<12} {'MP3 Value':<12} {'XOR':<12} {'Bit Diff':<8}")
            print("-" * 50)
            for diff in fp_analysis['first_10_differences'][:5]:  # Show first 5
                print(f"{diff['position']:<4} {diff['value1']:<12} {diff['value2']:<12} {diff['xor']:<12} {diff['bit_differences']:<8}")
    
    # 5. Content comparison
    print(f"\n📊 CONTENT COMPARISON:")
    content_analysis = compare_audio_content(wav_file, mp3_file)
    if 'error' not in content_analysis:
        print(f"Normalized sample sizes: WAV={content_analysis['sample1_size']} bytes, MP3={content_analysis['sample2_size']} bytes")
        print(f"Sample size ratio: {content_analysis['size_ratio']:.3f}")
        print(f"Sample size difference: {content_analysis['size_difference_percent']:.1f}%")
    else:
        print(f"Content comparison error: {content_analysis['error']}")
    
    # 6. Metadata comparison
    print(f"\n🏷️  METADATA COMPARISON:")
    wav_tags = wav_info.get('format_tags', {})
    mp3_tags = mp3_info.get('format_tags', {})
    
    all_tags = set(wav_tags.keys()) | set(mp3_tags.keys())
    if all_tags:
        print(f"{'Tag':<20} {'WAV':<30} {'MP3':<30}")
        print("-" * 80)
        for tag in sorted(all_tags):
            wav_val = wav_tags.get(tag, 'N/A')
            mp3_val = mp3_tags.get(tag, 'N/A')
            print(f"{tag:<20} {str(wav_val)[:29]:<30} {str(mp3_val)[:29]:<30}")
    else:
        print("No metadata tags found")
    
    # 7. Conclusions
    print(f"\n💡 ANALYSIS CONCLUSIONS:")
    
    duration_diff = abs(wav_info.get('duration', 0) - mp3_info.get('duration', 0))
    if duration_diff > 1.0:
        print(f"⚠️  Significant duration difference: {duration_diff:.2f}s")
        print("   This could indicate different versions or encoding issues")
    
    bitrate_ratio = mp3_info.get('bitrate', 1) / wav_info.get('bitrate', 1) if wav_info.get('bitrate', 0) > 0 else 0
    if bitrate_ratio < 0.5:
        print(f"⚠️  Very low MP3 bitrate ratio: {bitrate_ratio:.3f}")
        print("   Heavy compression may be causing fingerprint differences")
    
    if fp_analysis.get('bit_similarity', 0) < 0.85:
        print(f"⚠️  Low bit-level fingerprint similarity: {fp_analysis.get('bit_similarity', 0):.3f}")
        print("   This suggests significant acoustic differences")
    
    if similarity < 0.9:
        print(f"⚠️  Lower than expected similarity for format conversion: {similarity:.6f}")
        print("   Possible causes:")
        print("   - Different source material or versions")
        print("   - Significant compression artifacts")
        print("   - Different mastering or processing")
        print("   - Timing differences or sample rate conversion issues")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
