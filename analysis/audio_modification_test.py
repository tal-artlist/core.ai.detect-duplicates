#!/usr/bin/env python3
"""
Test how Chromaprint handles various audio modifications
This will help us understand robustness for real-world duplicate detection
"""

import os
import sys
import subprocess

# Auto-restart with correct environment if needed
if 'DYLD_LIBRARY_PATH' not in os.environ or '/opt/homebrew/lib' not in os.environ.get('DYLD_LIBRARY_PATH', ''):
    import subprocess
    env = os.environ.copy()
    env['DYLD_LIBRARY_PATH'] = '/opt/homebrew/lib:' + env.get('DYLD_LIBRARY_PATH', '')
    result = subprocess.run([sys.executable] + sys.argv, env=env)
    sys.exit(result.returncode)

import ctypes
from pathlib import Path
import pandas as pd
import tempfile
import shutil

def setup_chromaprint():
    """Set up Chromaprint library and environment"""
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

def get_audio_info(file_path):
    """Get basic audio file information using ffprobe"""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams',
            str(file_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        import json
        data = json.loads(result.stdout)
        
        audio_stream = next((s for s in data['streams'] if s['codec_type'] == 'audio'), None)
        if audio_stream:
            return {
                'duration': float(data['format']['duration']),
                'bitrate': int(data['format'].get('bit_rate', 0)),
                'sample_rate': int(audio_stream.get('sample_rate', 0)),
                'channels': int(audio_stream.get('channels', 0)),
                'codec': audio_stream.get('codec_name', 'unknown')
            }
    except Exception as e:
        print(f"Warning: Could not get audio info for {file_path}: {e}")
        return {}

def create_modified_versions(original_file, temp_dir):
    """Create various modified versions of the audio file"""
    modifications = []
    base_name = Path(original_file).stem
    
    # Get original duration first
    original_info = get_audio_info(original_file)
    original_duration = original_info.get('duration', 0)
    
    print(f"Original file duration: {original_duration:.1f}s")
    
    # 1. Exact copy (should be 1.0 similarity)
    copy_file = temp_dir / f"{base_name}_exact_copy.mp3"
    shutil.copy2(original_file, copy_file)
    modifications.append(('Exact Copy', copy_file, 'Identical file copy'))
    
    # 2. Trim start (remove first 10 seconds)
    trim_start_file = temp_dir / f"{base_name}_trim_start.mp3"
    cmd = ['ffmpeg', '-y', '-i', str(original_file), '-ss', '10', '-c', 'copy', str(trim_start_file)]
    subprocess.run(cmd, capture_output=True, check=True)
    modifications.append(('Trim Start (10s)', trim_start_file, 'Removed first 10 seconds'))
    
    # 3. Trim end (remove last 10 seconds)
    if original_duration > 20:
        trim_end_file = temp_dir / f"{base_name}_trim_end.mp3"
        end_time = original_duration - 10
        cmd = ['ffmpeg', '-y', '-i', str(original_file), '-t', str(end_time), '-c', 'copy', str(trim_end_file)]
        subprocess.run(cmd, capture_output=True, check=True)
        modifications.append(('Trim End (10s)', trim_end_file, 'Removed last 10 seconds'))
    
    # 4. Trim middle (remove 10 seconds from middle)
    if original_duration > 30:
        trim_middle_file = temp_dir / f"{base_name}_trim_middle.mp3"
        start_time = original_duration / 2 - 5
        end_time = original_duration / 2 + 5
        # Create two parts and concatenate
        part1 = temp_dir / f"{base_name}_part1.mp3"
        part2 = temp_dir / f"{base_name}_part2.mp3"
        
        # First part (0 to middle-5s)
        cmd1 = ['ffmpeg', '-y', '-i', str(original_file), '-t', str(start_time), '-c', 'copy', str(part1)]
        subprocess.run(cmd1, capture_output=True, check=True)
        
        # Second part (middle+5s to end)
        cmd2 = ['ffmpeg', '-y', '-i', str(original_file), '-ss', str(end_time), '-c', 'copy', str(part2)]
        subprocess.run(cmd2, capture_output=True, check=True)
        
        # Concatenate
        concat_file = temp_dir / 'concat_list.txt'
        with open(concat_file, 'w') as f:
            f.write(f"file '{part1}'\n")
            f.write(f"file '{part2}'\n")
        
        cmd3 = ['ffmpeg', '-y', '-f', 'concat', '-safe', '0', '-i', str(concat_file), '-c', 'copy', str(trim_middle_file)]
        subprocess.run(cmd3, capture_output=True, check=True)
        modifications.append(('Trim Middle (10s)', trim_middle_file, 'Removed 10s from middle'))
    
    # 5. Low quality MP3 (64 kbps)
    low_quality_file = temp_dir / f"{base_name}_low_quality.mp3"
    cmd = ['ffmpeg', '-y', '-i', str(original_file), '-b:a', '64k', str(low_quality_file)]
    subprocess.run(cmd, capture_output=True, check=True)
    modifications.append(('Low Quality (64k)', low_quality_file, 'Compressed to 64 kbps'))
    
    # 6. Very low quality MP3 (32 kbps)
    very_low_quality_file = temp_dir / f"{base_name}_very_low_quality.mp3"
    cmd = ['ffmpeg', '-y', '-i', str(original_file), '-b:a', '32k', str(very_low_quality_file)]
    subprocess.run(cmd, capture_output=True, check=True)
    modifications.append(('Very Low Quality (32k)', very_low_quality_file, 'Compressed to 32 kbps'))
    
    # 7. Mono conversion
    mono_file = temp_dir / f"{base_name}_mono.mp3"
    cmd = ['ffmpeg', '-y', '-i', str(original_file), '-ac', '1', str(mono_file)]
    subprocess.run(cmd, capture_output=True, check=True)
    modifications.append(('Mono Conversion', mono_file, 'Converted to mono'))
    
    # 8. Sample rate change (22kHz)
    low_sample_rate_file = temp_dir / f"{base_name}_22khz.mp3"
    cmd = ['ffmpeg', '-y', '-i', str(original_file), '-ar', '22050', str(low_sample_rate_file)]
    subprocess.run(cmd, capture_output=True, check=True)
    modifications.append(('Low Sample Rate (22kHz)', low_sample_rate_file, 'Resampled to 22kHz'))
    
    # 9. Volume change (+6dB)
    loud_file = temp_dir / f"{base_name}_loud.mp3"
    cmd = ['ffmpeg', '-y', '-i', str(original_file), '-filter:a', 'volume=6dB', str(loud_file)]
    subprocess.run(cmd, capture_output=True, check=True)
    modifications.append(('Volume +6dB', loud_file, 'Increased volume by 6dB'))
    
    # 10. Volume change (-6dB)
    quiet_file = temp_dir / f"{base_name}_quiet.mp3"
    cmd = ['ffmpeg', '-y', '-i', str(original_file), '-filter:a', 'volume=-6dB', str(quiet_file)]
    subprocess.run(cmd, capture_output=True, check=True)
    modifications.append(('Volume -6dB', quiet_file, 'Decreased volume by 6dB'))
    
    # 11. Speed change (1.1x faster)
    fast_file = temp_dir / f"{base_name}_fast.mp3"
    cmd = ['ffmpeg', '-y', '-i', str(original_file), '-filter:a', 'atempo=1.1', str(fast_file)]
    subprocess.run(cmd, capture_output=True, check=True)
    modifications.append(('Speed 1.1x', fast_file, '10% faster playback'))
    
    # 12. Speed change (0.9x slower)
    slow_file = temp_dir / f"{base_name}_slow.mp3"
    cmd = ['ffmpeg', '-y', '-i', str(original_file), '-filter:a', 'atempo=0.9', str(slow_file)]
    subprocess.run(cmd, capture_output=True, check=True)
    modifications.append(('Speed 0.9x', slow_file, '10% slower playback'))
    
    # 13. Add silence at start (5 seconds)
    silence_start_file = temp_dir / f"{base_name}_silence_start.mp3"
    cmd = ['ffmpeg', '-y', '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100', '-i', str(original_file), '-filter_complex', '[0:a][1:a]concat=n=2:v=0:a=1[out]', '-map', '[out]', '-t', str(original_duration + 5), str(silence_start_file)]
    subprocess.run(cmd, capture_output=True, check=True)
    modifications.append(('Silence at Start (5s)', silence_start_file, 'Added 5s silence at beginning'))
    
    return modifications

def analyze_file(acoustid, file_path):
    """Analyze a single file and return fingerprint info"""
    try:
        duration, fingerprint = acoustid.fingerprint_file(str(file_path))
        audio_info = get_audio_info(file_path)
        
        return {
            'file_path': file_path,
            'duration': duration,
            'fingerprint': fingerprint,
            'file_size_mb': file_path.stat().st_size / (1024*1024),
            'success': True,
            'error': None,
            **audio_info
        }
    except Exception as e:
        return {
            'file_path': file_path,
            'duration': None,
            'fingerprint': None,
            'file_size_mb': 0,
            'success': False,
            'error': str(e)
        }

def main():
    print("=== Audio Modification Robustness Test ===\n")
    
    acoustid = setup_chromaprint()
    if not acoustid:
        return 1
    
    # Choose a test file (use one of our existing files)
    test_files = [
        "samples/artlist/735301_Nobou_-_Hope_And_Glory_-_Master_-_84_BPM_-_300822_-_EXT_-_2444.mp3",
        "samples/artlist/734026_730212_724283_Avishai_Rozen_-_Seasonal_Beats_-_AO-000336_-_Master_v3_-_260722_-_IV_-_ORG_-_2444.mp3",
        "samples/artlist/10001_74259_74259_Soft_In_the_Head_-16-44.1-.mp3"
    ]
    
    original_file = None
    for test_file in test_files:
        if Path(test_file).exists():
            original_file = Path(test_file)
            break
    
    if not original_file:
        print("❌ No test files found")
        return 1
    
    print(f"🎵 Using test file: {original_file.name}")
    
    # Create temporary directory for modifications
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        print(f"📁 Creating modified versions in: {temp_path}")
        
        try:
            # Create all modified versions
            modifications = create_modified_versions(original_file, temp_path)
            print(f"✅ Created {len(modifications)} modified versions")
            
            # Analyze original file
            print(f"\n🔍 Analyzing original file...")
            original_analysis = analyze_file(acoustid, original_file)
            
            if not original_analysis['success']:
                print(f"❌ Failed to analyze original file: {original_analysis['error']}")
                return 1
            
            # Analyze all modified versions
            print(f"🔍 Analyzing modified versions...")
            results = []
            
            for mod_name, mod_file, description in modifications:
                if not mod_file.exists():
                    print(f"⚠️  Skipping {mod_name}: file not created")
                    continue
                
                print(f"  Processing: {mod_name}")
                analysis = analyze_file(acoustid, mod_file)
                
                if analysis['success']:
                    # Compare with original
                    try:
                        similarity = acoustid.compare_fingerprints(
                            (original_analysis['duration'], original_analysis['fingerprint']),
                            (analysis['duration'], analysis['fingerprint'])
                        )
                        
                        results.append({
                            'Modification': mod_name,
                            'Description': description,
                            'Similarity': similarity,
                            'Original_Duration': original_analysis['duration'],
                            'Modified_Duration': analysis['duration'],
                            'Duration_Change': analysis['duration'] - original_analysis['duration'],
                            'Original_Size_MB': original_analysis['file_size_mb'],
                            'Modified_Size_MB': analysis['file_size_mb'],
                            'Size_Ratio': analysis['file_size_mb'] / original_analysis['file_size_mb'],
                            'Original_Bitrate': original_analysis.get('bitrate', 0),
                            'Modified_Bitrate': analysis.get('bitrate', 0),
                            'Status': 'Success'
                        })
                        
                    except Exception as e:
                        results.append({
                            'Modification': mod_name,
                            'Description': description,
                            'Similarity': None,
                            'Status': f'Comparison Error: {e}'
                        })
                else:
                    results.append({
                        'Modification': mod_name,
                        'Description': description,
                        'Similarity': None,
                        'Status': f'Analysis Error: {analysis["error"]}'
                    })
            
            # Create results table
            df = pd.DataFrame(results)
            
            # Display results
            print(f"\n📊 ROBUSTNESS TEST RESULTS")
            print(f"{'='*80}")
            print(f"Original file: {original_file.name}")
            print(f"Original duration: {original_analysis['duration']:.1f}s")
            print(f"Original size: {original_analysis['file_size_mb']:.1f} MB")
            print(f"Original bitrate: {original_analysis.get('bitrate', 0)} bps")
            print(f"{'='*80}")
            
            # Sort by similarity (descending)
            df_sorted = df[df['Status'] == 'Success'].sort_values('Similarity', ascending=False)
            
            print(f"\n🎯 SIMILARITY RESULTS (sorted by similarity):")
            print(f"{'Rank':<4} {'Modification':<25} {'Similarity':<12} {'Duration Δ':<12} {'Size Ratio':<10} {'Description'}")
            print("-" * 100)
            
            for i, (_, row) in enumerate(df_sorted.iterrows(), 1):
                similarity_str = f"{row['Similarity']:.6f}" if row['Similarity'] is not None else "ERROR"
                duration_change = f"{row['Duration_Change']:+.1f}s" if 'Duration_Change' in row else "N/A"
                size_ratio = f"{row['Size_Ratio']:.2f}x" if 'Size_Ratio' in row else "N/A"
                
                print(f"{i:<4} {row['Modification']:<25} {similarity_str:<12} {duration_change:<12} {size_ratio:<10} {row['Description']}")
            
            # Analysis by similarity ranges
            successful_results = df_sorted['Similarity'].dropna()
            if len(successful_results) > 0:
                print(f"\n📈 SIMILARITY ANALYSIS:")
                print(f"   Total successful comparisons: {len(successful_results)}")
                print(f"   Highest similarity: {successful_results.max():.6f}")
                print(f"   Lowest similarity: {successful_results.min():.6f}")
                print(f"   Average similarity: {successful_results.mean():.6f}")
                print(f"   Median similarity: {successful_results.median():.6f}")
                
                # Categorize results
                identical = len(successful_results[successful_results >= 0.99])
                very_high = len(successful_results[(successful_results >= 0.90) & (successful_results < 0.99)])
                high = len(successful_results[(successful_results >= 0.80) & (successful_results < 0.90)])
                medium = len(successful_results[(successful_results >= 0.50) & (successful_results < 0.80)])
                low = len(successful_results[successful_results < 0.50])
                
                print(f"\n🎯 SIMILARITY DISTRIBUTION:")
                print(f"   Identical (≥0.99): {identical} modifications")
                print(f"   Very High (0.90-0.99): {very_high} modifications")
                print(f"   High (0.80-0.90): {high} modifications")
                print(f"   Medium (0.50-0.80): {medium} modifications")
                print(f"   Low (<0.50): {low} modifications")
                
                print(f"\n💡 INSIGHTS FOR DUPLICATE DETECTION:")
                if identical > 0:
                    print(f"   • {identical} modifications still show as identical (≥0.99)")
                if very_high > 0:
                    print(f"   • {very_high} modifications show very high similarity (0.90-0.99)")
                if high > 0:
                    print(f"   • {high} modifications show high similarity (0.80-0.90)")
                if medium > 0:
                    print(f"   • {medium} modifications show medium similarity (0.50-0.80)")
                if low > 0:
                    print(f"   • {low} modifications show low similarity (<0.50)")
            
            # Save detailed results
            output_file = f"audio_modification_test_results.csv"
            df.to_csv(output_file, index=False)
            print(f"\n📁 Detailed results saved to: {output_file}")
            
        except Exception as e:
            print(f"❌ Error during processing: {e}")
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
