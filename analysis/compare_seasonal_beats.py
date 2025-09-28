#!/usr/bin/env python3
"""
Compare all variations of "Seasonal Beats" by Avishai Rozen
"""

import os
import sys

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

def analyze_file(acoustid, file_path):
    """Analyze a single file"""
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
            'success': True,
            'error': None
        }
    except Exception as e:
        return {
            'file': file_path.name,
            'path': str(file_path),
            'exists': file_path.exists(),
            'success': False,
            'error': str(e)
        }

def extract_version_info(filename):
    """Extract version information from filename"""
    version_info = {
        'master': 'Master_v3' in filename,
        'bov': '_BOV_' in filename,
        'sv_no_lead': 'SV_No_LeadVocals' in filename,
        'iv': '_IV_' in filename,
        'format': Path(filename).suffix.upper()
    }
    
    # Determine version type
    if version_info['sv_no_lead']:
        version_info['version'] = 'SV No LeadVocals'
    elif version_info['bov']:
        version_info['version'] = 'BOV'
    elif version_info['iv']:
        version_info['version'] = 'IV'
    else:
        version_info['version'] = 'Master'
    
    return version_info

def main():
    print("=== Seasonal Beats Variations Comparison ===\n")
    
    acoustid = setup_chromaprint()
    if not acoustid:
        return 1
    
    # Define all the files
    files = [
        "734026_730212_724283_Avishai_Rozen_-_Seasonal_Beats_-_AO-000336_-_Master_v3_-_260722_-_IV_-_ORG_-_2444.mp3",
        "734056_730200_724282_Avishai_Rozen_-_Seasonal_Beats_-_AO-000336_-_Master_v3_-_230522_-_BOV_-_ORG_-_2444.mp3",
        "734057_730202_724290_Avishai_Rozen_-_Seasonal_Beats_-_AO-000336_-_Master_v3_-_260722_-_SV_No_LeadVocals-ORG-2444.mp3",
        "734081_730200_724282_Avishai_Rozen_-_Seasonal_Beats_-_AO-000336_-_Master_v3_-_230522_-_BOV_-_ORG_-_2444.wav",
        "734082_730202_724290_Avishai_Rozen_-_Seasonal_Beats_-_AO-000336_-_Master_v3_-_260722_-_SV_No_LeadVocals-ORG-2444.wav",
        "734083_730212_724283_Avishai_Rozen_-_Seasonal_Beats_-_AO-000336_-_Master_v3_-_260722_-_IV_-_ORG_-_2444.wav"
    ]
    
    # Convert to full paths
    base_dir = Path("samples/artlist")
    file_paths = [base_dir / f for f in files]
    
    print("🎵 Analyzing all Seasonal Beats variations...")
    
    # Analyze all files
    results = []
    for file_path in file_paths:
        print(f"Processing: {file_path.name[:60]}...")
        result = analyze_file(acoustid, file_path)
        if result['success']:
            version_info = extract_version_info(file_path.name)
            result.update(version_info)
        results.append(result)
    
    successful_results = [r for r in results if r['success']]
    print(f"\n✅ Successfully processed {len(successful_results)}/{len(results)} files")
    
    if len(successful_results) < 2:
        print("❌ Need at least 2 successful files for comparison")
        return 1
    
    # Display file information
    print(f"\n📁 File Information:")
    print(f"{'File':<25} {'Version':<15} {'Format':<6} {'Size(MB)':<8} {'Duration':<8} {'FP Len'}")
    print("-" * 80)
    
    for result in successful_results:
        short_name = result['file'][:24]
        print(f"{short_name:<25} {result.get('version', 'Unknown'):<15} {result['format']:<6} {result['size_mb']:<8.1f} {result['duration']:<8.1f} {result['fingerprint_length']}")
    
    # Compare all pairs
    print(f"\n🔍 Comparing all pairs ({len(successful_results)} files):")
    print(f"{'File 1 (Version)':<30} {'File 2 (Version)':<30} {'Similarity':<12} {'Status'}")
    print("-" * 85)
    
    comparisons = []
    
    for i in range(len(successful_results)):
        for j in range(i + 1, len(successful_results)):
            file1 = successful_results[i]
            file2 = successful_results[j]
            
            try:
                fp1_tuple = (file1['duration'], file1['fingerprint'])
                fp2_tuple = (file2['duration'], file2['fingerprint'])
                similarity = acoustid.compare_fingerprints(fp1_tuple, fp2_tuple)
                
                # Determine status
                if similarity >= 0.95:
                    status = "🎯 NEARLY IDENTICAL"
                elif similarity >= 0.80:
                    status = "🔍 VERY SIMILAR"
                elif similarity >= 0.50:
                    status = "📊 SIMILAR"
                elif similarity >= 0.20:
                    status = "📈 SOMEWHAT SIMILAR"
                else:
                    status = "❌ DIFFERENT"
                
                # Create comparison record
                comparison = {
                    'file1': file1['file'],
                    'file2': file2['file'],
                    'version1': file1.get('version', 'Unknown'),
                    'version2': file2.get('version', 'Unknown'),
                    'format1': file1['format'],
                    'format2': file2['format'],
                    'similarity': similarity,
                    'status': status,
                    'same_version_diff_format': (file1.get('version') == file2.get('version') and file1['format'] != file2['format']),
                    'same_format_diff_version': (file1['format'] == file2['format'] and file1.get('version') != file2.get('version'))
                }
                comparisons.append(comparison)
                
                # Display
                version1_short = f"{file1.get('version', 'Unk')} ({file1['format']})"
                version2_short = f"{file2.get('version', 'Unk')} ({file2['format']})"
                print(f"{version1_short:<30} {version2_short:<30} {similarity:<12.6f} {status}")
                
            except Exception as e:
                print(f"{'ERROR':<30} {'ERROR':<30} {'ERROR':<12} ❌ {e}")
    
    # Analysis by comparison type
    print(f"\n📊 Analysis by Comparison Type:")
    
    same_version_diff_format = [c for c in comparisons if c['same_version_diff_format']]
    same_format_diff_version = [c for c in comparisons if c['same_format_diff_version']]
    
    if same_version_diff_format:
        print(f"\n🔄 Same Version, Different Format ({len(same_version_diff_format)} comparisons):")
        for comp in same_version_diff_format:
            print(f"   {comp['version1']} ({comp['format1']} vs {comp['format2']}): {comp['similarity']:.6f}")
    
    if same_format_diff_version:
        print(f"\n🎭 Same Format, Different Version ({len(same_format_diff_version)} comparisons):")
        for comp in same_format_diff_version:
            print(f"   {comp['format1']} ({comp['version1']} vs {comp['version2']}): {comp['similarity']:.6f}")
    
    # Summary statistics
    similarities = [c['similarity'] for c in comparisons]
    print(f"\n📈 Summary Statistics:")
    print(f"   Total comparisons: {len(similarities)}")
    print(f"   Average similarity: {sum(similarities)/len(similarities):.6f}")
    print(f"   Highest similarity: {max(similarities):.6f}")
    print(f"   Lowest similarity: {min(similarities):.6f}")
    
    # Count by similarity ranges
    very_high = len([s for s in similarities if s >= 0.95])
    high = len([s for s in similarities if 0.80 <= s < 0.95])
    medium = len([s for s in similarities if 0.50 <= s < 0.80])
    low = len([s for s in similarities if s < 0.50])
    
    print(f"\n🎯 Similarity Distribution:")
    print(f"   Very High (≥0.95): {very_high} comparisons")
    print(f"   High (0.80-0.95): {high} comparisons")
    print(f"   Medium (0.50-0.80): {medium} comparisons")
    print(f"   Low (<0.50): {low} comparisons")
    
    # Save results
    df_comparisons = pd.DataFrame(comparisons)
    output_file = "seasonal_beats_comparison.csv"
    df_comparisons.to_csv(output_file, index=False)
    print(f"\n📁 Detailed results saved to: {output_file}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
