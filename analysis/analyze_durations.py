#!/usr/bin/env python3
"""
Analyze duration distribution to optimize clustering
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

def setup_chromaprint():
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

def cluster_by_duration(fingerprints, tolerance=5.0):
    """Cluster fingerprints by duration"""
    sorted_fps = sorted(fingerprints, key=lambda x: x['duration'])
    
    clusters = []
    current_cluster = []
    
    for fp in sorted_fps:
        if not current_cluster:
            current_cluster = [fp]
        else:
            cluster_min = min(f['duration'] for f in current_cluster)
            cluster_max = max(f['duration'] for f in current_cluster)
            
            if (fp['duration'] >= cluster_min - tolerance and 
                fp['duration'] <= cluster_max + tolerance):
                current_cluster.append(fp)
            else:
                if len(current_cluster) > 1:
                    clusters.append(current_cluster)
                current_cluster = [fp]
    
    if len(current_cluster) > 1:
        clusters.append(current_cluster)
    
    return clusters

def main():
    print("=== Duration Analysis for Clustering Optimization ===\n")
    
    acoustid = setup_chromaprint()
    if not acoustid:
        return 1
    
    # Get files
    artlist_dir = Path('samples/artlist')
    files = list(artlist_dir.glob('*.mp3'))[:15]  # First 15 files
    
    # Generate fingerprints
    fingerprints = []
    for i, file_path in enumerate(files):
        try:
            duration, fingerprint = acoustid.fingerprint_file(str(file_path))
            fingerprints.append({
                'id': i,
                'file_name': file_path.name,
                'duration': duration,
                'fingerprint': fingerprint
            })
            print(f"✅ {file_path.name}: {duration:.1f}s")
        except Exception as e:
            print(f"❌ {file_path.name}: {e}")
    
    print(f"\n📊 Duration Analysis ({len(fingerprints)} files):")
    
    # Show sorted durations
    sorted_fps = sorted(fingerprints, key=lambda x: x['duration'])
    for fp in sorted_fps:
        print(f"  {fp['duration']:6.1f}s - {fp['file_name'][:60]}")
    
    # Test different tolerances
    print(f"\n🔍 Clustering Analysis:")
    for tolerance in [5, 10, 20, 30, 50]:
        clusters = cluster_by_duration(fingerprints, tolerance)
        total_comparisons = sum(len(c) * (len(c) - 1) // 2 for c in clusters)
        naive_comparisons = len(fingerprints) * (len(fingerprints) - 1) // 2
        reduction = (1 - total_comparisons/naive_comparisons) * 100 if naive_comparisons > 0 else 0
        
        print(f"\nTolerance {tolerance}s:")
        print(f"  Clusters: {len(clusters)}")
        print(f"  Comparisons: {total_comparisons} (vs {naive_comparisons} naive)")
        print(f"  Reduction: {reduction:.1f}%")
        
        for i, cluster in enumerate(clusters):
            durations = [f['duration'] for f in cluster]
            print(f"    Cluster {i+1}: {len(cluster)} files ({min(durations):.1f}s - {max(durations):.1f}s)")
    
    # Find optimal tolerance
    print(f"\n💡 Recommendations:")
    durations = [fp['duration'] for fp in fingerprints]
    duration_range = max(durations) - min(durations)
    avg_gap = duration_range / len(durations) if len(durations) > 1 else 0
    
    print(f"  Duration range: {min(durations):.1f}s - {max(durations):.1f}s ({duration_range:.1f}s)")
    print(f"  Average gap: {avg_gap:.1f}s")
    print(f"  Suggested tolerance: {max(10, avg_gap * 2):.0f}s")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
