#!/usr/bin/env python3
"""
Simple Duplicate Analysis - Clean and focused duplicate detection
"""

import os
import sys
import json
import csv
import time
from datetime import datetime
from collections import Counter, defaultdict
from snowflake_utils import SnowflakeConnector

def load_fingerprints():
    """Load all fingerprints from Snowflake"""
    snowflake = SnowflakeConnector()
    query = """
    SELECT ASSET_ID, FILE_KEY, FORMAT, DURATION, FINGERPRINT, SOURCE
    FROM AI_DATA.AUDIO_FINGERPRINT
    WHERE PROCESSING_STATUS = 'SUCCESS' AND FINGERPRINT IS NOT NULL AND DURATION > 0
    ORDER BY SOURCE, ASSET_ID
    """
    
    print("Loading fingerprints...")
    cursor = snowflake.execute_query(query)
    
    fingerprints = []
    for row in cursor:
        fingerprints.append({
            'asset_id': row[0], 'file_key': row[1], 'format': row[2],
            'duration': float(row[3]), 'fingerprint': row[4], 'source': row[5]
        })
    
    cursor.close()
    snowflake.close()
    
    print(f"Loaded {len(fingerprints):,} fingerprints")
    return fingerprints

def find_exact_duplicates(fingerprints):
    """Find exact duplicates, filtering out same-asset format variations"""
    print("Finding exact duplicates...")
    
    # Group by fingerprint
    fp_groups = defaultdict(list)
    for fp in fingerprints:
        fp_groups[fp['fingerprint']].append(fp)
    
    # Keep only duplicates, filter out same-asset format variations
    duplicates = {}
    for fp_str, files in fp_groups.items():
        if len(files) < 2:
            continue
            
        # Group by (asset_id, source) to remove format variations
        asset_groups = defaultdict(list)
        for file in files:
            asset_groups[(file['asset_id'], file['source'])].append(file)
        
        # Keep one representative per asset/source
        unique_files = [group[0] for group in asset_groups.values()]
        
        # Only keep if multiple different assets
        if len(unique_files) > 1:
            duplicates[fp_str] = unique_files
    
    print(f"Found {len(duplicates):,} fingerprints with true duplicates")
    return duplicates

def export_and_analyze(duplicates):
    """Export duplicates to CSV and show summary stats"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    evaluation_dir = "evaluation"
    os.makedirs(evaluation_dir, exist_ok=True)
    filename = os.path.join(evaluation_dir, f"duplicate_groups_{timestamp}.csv")
    
    # Sort by copy count (highest first)
    sorted_dups = sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Export to CSV
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'group_id', 'copy_count', 'is_cross_source', 'sources', 
            'assets', 'fingerprint_preview'
        ])
        
        # Data rows
        duplicate_data = []
        for i, (fp_str, files) in enumerate(sorted_dups, 1):
            sources = list(set(f['source'] for f in files))
            is_cross_source = len(sources) > 1
            
            duplicate_data.append({
                "copy_count": len(files),
                "is_cross_source": is_cross_source,
                "sources": sources,
                "assets": [f['asset_id'] for f in files]
            })
            
            writer.writerow([
                i,
                len(files),
                is_cross_source,
                ', '.join(sources),
                ', '.join(map(str, [f['asset_id'] for f in files])),
                fp_str[:50] + '...'
            ])
    
    # Quick stats
    copy_counts = Counter(len(files) for files in duplicates.values())
    cross_source = sum(1 for d in duplicate_data if d["is_cross_source"])
    
    print(f"\n📊 RESULTS:")
    print(f"Exported to: {filename}")
    print(f"Total duplicate groups: {len(duplicates):,}")
    print(f"Cross-source duplicates: {cross_source:,}")
    
    print(f"\n📈 Copy distribution:")
    for count in sorted(copy_counts.keys(), reverse=True)[:10]:
        print(f"  {count} copies: {copy_counts[count]:,} groups")
    
    print(f"\n🔍 Top 5 highest copy counts:")
    for i, (fp_str, files) in enumerate(sorted_dups[:5], 1):
        sources = list(set(f['source'] for f in files))
        cross = "🌐" if len(sources) > 1 else "🏠"
        assets = [f['asset_id'] for f in files]
        assets_str = str(assets[:5])
        if len(assets) > 5:
            assets_str = assets_str[:-1] + ", ...]"
        print(f"  {i}. {len(files)} copies {cross} - Assets: {assets_str}")
    
    return filename, duplicate_data

def find_high_copies(data, min_copies=10):
    """Find cases with unusually high copy counts"""
    return [d for d in data if d["copy_count"] >= min_copies]

def find_cross_source(data):
    """Find cross-source duplicates"""
    return [d for d in data if d["is_cross_source"]]

def search_asset(data, asset_id):
    """Find duplicates involving specific asset"""
    return [d for d in data if asset_id in d["assets"]]

def main():
    """Main analysis workflow"""
    print("🔍 Simple Duplicate Analysis")
    print("=" * 50)
    
    # Load data
    fingerprints = load_fingerprints()
    
    # Find duplicates
    duplicates = find_exact_duplicates(fingerprints)
    
    # Export and analyze
    filename, data = export_and_analyze(duplicates)
    
    # Show weird cases immediately
    weird_cases = find_high_copies(data, 50)
    if weird_cases:
        print(f"\n⚠️  Found {len(weird_cases)} cases with 50+ copies:")
        for case in weird_cases:
            assets_str = str(case['assets'][:10])
            if len(case['assets']) > 10:
                assets_str = assets_str[:-1] + ", ...]"
            print(f"  {case['copy_count']} copies - Assets: {assets_str}")
    
    print(f"\n🔍 Analysis functions available:")
    print(f"  find_high_copies(data, 20)  # Find 20+ copy cases")
    print(f"  find_cross_source(data)     # Find cross-source duplicates")
    print(f"  search_asset(data, 12345)   # Find asset 12345 duplicates")
    
    return data

if __name__ == "__main__":
    data = main()
