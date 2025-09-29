#!/usr/bin/env python3
"""
Test the optimized duplicate detection approach with existing files
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
import time
import pandas as pd
import sqlite3
from typing import List, Dict
from collections import defaultdict

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

def cluster_by_duration(fingerprints: List[Dict], tolerance: float = 5.0) -> List[List[Dict]]:
    """Cluster fingerprints by duration to reduce comparison complexity"""
    
    # Sort by duration
    sorted_fps = sorted(fingerprints, key=lambda x: x['duration'])
    
    clusters = []
    current_cluster = []
    
    for fp in sorted_fps:
        if not current_cluster:
            current_cluster = [fp]
        else:
            # Check if this fingerprint fits in current cluster
            cluster_min = min(f['duration'] for f in current_cluster)
            cluster_max = max(f['duration'] for f in current_cluster)
            
            if (fp['duration'] >= cluster_min - tolerance and 
                fp['duration'] <= cluster_max + tolerance):
                current_cluster.append(fp)
            else:
                # Start new cluster
                if len(current_cluster) > 1:  # Only keep clusters with multiple items
                    clusters.append(current_cluster)
                current_cluster = [fp]
    
    # Don't forget the last cluster
    if len(current_cluster) > 1:
        clusters.append(current_cluster)
    
    return clusters

def classify_duplicate_type(song1: Dict, song2: Dict, similarity: float) -> str:
    """Classify the type of duplicate based on our learnings"""
    
    same_format = song1['format'] == song2['format']
    
    if similarity >= 0.99:
        return "IDENTICAL" if same_format else "SAME_CONTENT_DIFF_FORMAT"
    elif similarity >= 0.90:
        return "VERY_HIGH_SIMILARITY"
    elif similarity >= 0.80:
        return "HIGH_SIMILARITY"  # Like our 0.807 WAV/MP3 case
    elif similarity >= 0.60:
        return "RELATED_VERSIONS"  # Like our 0.667 IV/SV case
    else:
        return "DIFFERENT_ARRANGEMENTS"  # Like our 0.043 BOV case

def main():
    print("=== Testing Optimized Duplicate Detection ===\n")
    
    acoustid = setup_chromaprint()
    if not acoustid:
        return 1
    
    # Find existing audio files
    artlist_dir = Path("samples/artlist")
    motionarray_dir = Path("samples/motionarray")
    
    all_files = []
    
    # Collect files from both directories
    if artlist_dir.exists():
        artlist_files = list(artlist_dir.glob("*.mp3")) + list(artlist_dir.glob("*.wav"))
        all_files.extend([(f, "artlist") for f in artlist_files])  # Use ALL files
        print(f"📁 Found {len(artlist_files)} Artlist files (using ALL)")
    
    if motionarray_dir.exists():
        motionarray_files = list(motionarray_dir.glob("*.mp3")) + list(motionarray_dir.glob("*.wav"))
        all_files.extend([(f, "motionarray") for f in motionarray_files])  # Use ALL files
        print(f"📁 Found {len(motionarray_files)} MotionArray files (using ALL)")
    
    if not all_files:
        print("❌ No audio files found")
        return 1
    
    print(f"🎵 Processing {len(all_files)} files total")
    
    # Phase 1: Generate fingerprints
    print(f"\n📊 Phase 1: Fingerprint Generation")
    fingerprints = []
    
    for i, (file_path, source) in enumerate(all_files):
        print(f"Processing {i+1}/{len(all_files)}: {file_path.name}")
        
        try:
            start_time = time.time()
            duration, fingerprint = acoustid.fingerprint_file(str(file_path))
            processing_time = time.time() - start_time
            
            fingerprints.append({
                'id': i,
                'file_path': str(file_path),
                'file_name': file_path.name,
                'source': source,
                'duration': duration,
                'file_size': file_path.stat().st_size,
                'format': file_path.suffix.lower(),
                'fingerprint': fingerprint,
                'processing_time': processing_time
            })
            
            print(f"  ✅ {duration:.1f}s, {processing_time:.2f}s processing")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
    
    print(f"✅ Generated {len(fingerprints)} fingerprints")
    
    # Phase 2: Smart clustering
    print(f"\n🔍 Phase 2: Smart Duration Clustering")
    clusters = cluster_by_duration(fingerprints, tolerance=5.0)  # Use 5s tolerance for precision
    
    total_comparisons_naive = len(fingerprints) * (len(fingerprints) - 1) // 2
    total_comparisons_smart = sum(len(c) * (len(c) - 1) // 2 for c in clusters)
    
    print(f"📈 Optimization Results:")
    print(f"   Naive approach: {total_comparisons_naive:,} comparisons")
    print(f"   Smart clustering: {total_comparisons_smart:,} comparisons")
    print(f"   Reduction: {(1 - total_comparisons_smart/total_comparisons_naive)*100:.1f}%")
    print(f"   Duration clusters: {len(clusters)}")
    
    # Show cluster details
    print(f"\n📊 Cluster Details:")
    for i, cluster in enumerate(clusters):
        durations = [f['duration'] for f in cluster]
        print(f"   Cluster {i+1}: {len(cluster)} files, duration range: {min(durations):.1f}s - {max(durations):.1f}s")
    
    # Phase 3: Find duplicates
    print(f"\n🔍 Phase 3: Duplicate Detection")
    all_duplicates = []
    
    for i, cluster in enumerate(clusters):
        print(f"Processing cluster {i+1}/{len(clusters)} ({len(cluster)} files)...")
        
        cluster_duplicates = []
        comparisons_in_cluster = 0
        
        for j in range(len(cluster)):
            for k in range(j + 1, len(cluster)):
                song1, song2 = cluster[j], cluster[k]
                comparisons_in_cluster += 1
                
                # Quick pre-check: skip if very different file sizes
                if song1['file_size'] > 0 and song2['file_size'] > 0:
                    size_ratio = max(song1['file_size'], song2['file_size']) / \
                               min(song1['file_size'], song2['file_size'])
                    if size_ratio > 10:  # Skip if 10x size difference
                        continue
                
                try:
                    # Actual fingerprint comparison
                    similarity = acoustid.compare_fingerprints(
                        (song1['duration'], song1['fingerprint']),
                        (song2['duration'], song2['fingerprint'])
                    )
                    
                    if similarity >= 0.75:  # Lower threshold for testing
                        duplicate_type = classify_duplicate_type(song1, song2, similarity)
                        
                        duplicate = {
                            'song1_name': song1['file_name'],
                            'song2_name': song2['file_name'],
                            'similarity': similarity,
                            'duplicate_type': duplicate_type,
                            'duration1': song1['duration'],
                            'duration2': song2['duration'],
                            'source1': song1['source'],
                            'source2': song2['source'],
                            'format1': song1['format'],
                            'format2': song2['format']
                        }
                        
                        cluster_duplicates.append(duplicate)
                        all_duplicates.append(duplicate)
                        
                        print(f"    🔍 Found: {song1['file_name'][:30]} vs {song2['file_name'][:30]} ({similarity:.4f})")
                        
                except Exception as e:
                    print(f"    ❌ Error comparing: {e}")
        
        print(f"  ✅ Cluster {i+1}: {len(cluster_duplicates)} duplicates from {comparisons_in_cluster} comparisons")
    
    # Results summary
    print(f"\n📊 Final Results:")
    print(f"   Total files processed: {len(fingerprints)}")
    print(f"   Total duplicates found: {len(all_duplicates)}")
    print(f"   Total comparisons: {total_comparisons_smart:,}")
    
    if all_duplicates:
        # Group by duplicate type
        by_type = defaultdict(int)
        for dup in all_duplicates:
            by_type[dup['duplicate_type']] += 1
        
        print(f"\n🎯 Duplicates by Type:")
        for dup_type, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
            print(f"   {dup_type}: {count}")
        
        # Show top duplicates
        print(f"\n🔍 All Duplicates Found:")
        sorted_duplicates = sorted(all_duplicates, key=lambda x: x['similarity'], reverse=True)
        for i, dup in enumerate(sorted_duplicates, 1):
            print(f"   {i:2}. {dup['similarity']:.4f} - {dup['song1_name'][:40]} vs {dup['song2_name'][:40]}")
            print(f"       Type: {dup['duplicate_type']}, Sources: {dup['source1']} vs {dup['source2']}")
        
        # Save to CSV
        df = pd.DataFrame(all_duplicates)
        output_file = "test_duplicates_found.csv"
        df.to_csv(output_file, index=False)
        print(f"\n📁 Results saved to: {output_file}")
    
    print(f"\n✅ Test complete!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
