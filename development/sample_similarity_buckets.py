#!/usr/bin/env python3
"""
Sample Similarity Buckets - Download random audio pairs from different similarity ranges
Usage: python sample_similarity_buckets.py [--samples 5] [--buckets 0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
"""

import json
import os
import random
import requests
import argparse
from collections import defaultdict
from datetime import datetime
from snowflake_utils import SnowflakeConnector

def get_download_url_from_api(file_key: str, source: str):
    """Get signed download URL from Artlist/MotionArray API"""
    try:
        api_url = "https://oapi-int.artlist.io/v1/content/bulkDownloadArtifacts"
        headers = {
            'service-host': 'core.content.cms.api',
            'Content-Type': 'application/json'
        }
        
        payload = {"keys": [file_key]}
        
        response = requests.post(
            api_url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        response.raise_for_status()
        result = response.json()
        
        if 'data' in result and 'downloadArtifactResponses' in result['data']:
            responses = result['data']['downloadArtifactResponses']
            for key, response in responses.items():
                if isinstance(response, dict) and 'url' in response:
                    return response['url']
        
        return None
        
    except Exception as e:
        print(f"   ❌ API request failed for {file_key}: {e}")
        return None

def download_file(url, output_path):
    """Download file from URL"""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
            return True
        else:
            return False
            
    except Exception as e:
        print(f"   ❌ Download error: {e}")
        return False

def load_and_bucket_pairs(jsonl_file, bucket_thresholds, same_format_only=False, cross_source_only=False, same_source_only=False, on_air_only=False):
    """Load pairs and organize into similarity buckets"""
    print(f"📂 Loading pairs from {jsonl_file}...")
    if same_format_only:
        print(f"   🔍 Filtering: same format only (mp3→mp3, wav→wav, etc.)")
    if cross_source_only:
        print(f"   🔍 Filtering: cross-source only (artlist↔motionarray)")
    if same_source_only:
        print(f"   🔍 Filtering: same-source only (artlist↔artlist, motionarray↔motionarray)")
    if on_air_only:
        print(f"   🔍 Filtering: on-air assets only (LAST_STATUS_GROUP='ON_AIR')")
    
    # Create buckets: [(0.0, 0.1), (0.1, 0.2), ...]
    buckets = []
    for i in range(len(bucket_thresholds) - 1):
        buckets.append((bucket_thresholds[i], bucket_thresholds[i + 1]))
    
    bucketed = {bucket: [] for bucket in buckets}
    
    total_pairs = 0
    filtered_pairs = 0
    skipped = 0
    on_air_passed = 0
    format_stats = defaultdict(lambda: 0)
    source_stats = defaultdict(lambda: 0)
    
    # If on-air filtering is requested, get all unique asset IDs first
    on_air_assets = set()
    if on_air_only:
        print(f"   🔄 First pass: collecting all asset IDs...")
        all_asset_ids = set()
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    pair = json.loads(line)
                    all_asset_ids.add(pair['asset_id_1'])
                    all_asset_ids.add(pair['asset_id_2'])
                except:
                    pass
        print(f"   📋 Found {len(all_asset_ids):,} unique assets in file")
        on_air_assets = get_on_air_assets_from_snowflake(list(all_asset_ids))
        print()
    
    with open(jsonl_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                pair = json.loads(line)
                
                # Filter by format if requested
                if same_format_only:
                    if pair.get('format_1') != pair.get('format_2'):
                        filtered_pairs += 1
                        continue
                    format_stats[pair.get('format_1')] += 1
                
                # Filter by source if requested
                if cross_source_only:
                    source_1 = pair.get('source_1')
                    source_2 = pair.get('source_2')
                    if source_1 == source_2:
                        filtered_pairs += 1
                        continue
                    source_pair = f"{source_1}↔{source_2}"
                    source_stats[source_pair] += 1
                
                # Filter for same source if requested
                if same_source_only:
                    source_1 = pair.get('source_1')
                    source_2 = pair.get('source_2')
                    if source_1 != source_2:
                        filtered_pairs += 1
                        continue
                    source_pair = f"{source_1}↔{source_2}"
                    source_stats[source_pair] += 1
                
                # Filter by on-air status if requested
                if on_air_only:
                    asset_id_1 = pair.get('asset_id_1')
                    asset_id_2 = pair.get('asset_id_2')
                    source_1 = pair.get('source_1')
                    source_2 = pair.get('source_2')
                    
                    # Debug: show first few filtered pairs
                    check_1 = (asset_id_1, source_1) in on_air_assets
                    check_2 = (asset_id_2, source_2) in on_air_assets
                    
                    if not check_1 or not check_2:
                        if filtered_pairs < 5:  # Only show first 5
                            print(f"   🔍 DEBUG: Filtered pair {filtered_pairs+1}:")
                            print(f"      Asset 1: {asset_id_1} ({source_1}) - on-air: {check_1}")
                            print(f"      Asset 2: {asset_id_2} ({source_2}) - on-air: {check_2}")
                        filtered_pairs += 1
                        continue
                    else:
                        on_air_passed += 1
                
                similarity = pair['similarity']
                total_pairs += 1
            except json.JSONDecodeError as e:
                print(f"   ⚠️  Skipping malformed JSON on line {line_num}: {e}")
                skipped += 1
                continue
            
            # Find appropriate bucket
            placed = False
            for bucket_min, bucket_max in buckets:
                if bucket_min <= similarity < bucket_max:
                    bucketed[(bucket_min, bucket_max)].append(pair)
                    placed = True
                    break
                # Special case: include 1.0 in the last bucket
                elif similarity == bucket_max == buckets[-1][1]:
                    bucketed[(bucket_min, bucket_max)].append(pair)
                    placed = True
                    break
    
    print(f"   Loaded {total_pairs:,} total pairs")
    if same_format_only or cross_source_only or same_source_only or on_air_only:
        print(f"   ⚠️  Filtered out {filtered_pairs:,} pairs")
    if on_air_only:
        print(f"   ✅ Passed on-air check: {on_air_passed:,} pairs")
    if same_format_only:
        print(f"   📊 Same-format breakdown:")
        for fmt, count in sorted(format_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"      {fmt}: {count:,} pairs")
    if cross_source_only or same_source_only:
        print(f"   📊 Source breakdown:")
        for source_pair, count in sorted(source_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"      {source_pair}: {count:,} pairs")
    if skipped > 0:
        print(f"   ⚠️  Skipped {skipped:,} malformed lines")
    print(f"\n📊 Bucket distribution:")
    for (bucket_min, bucket_max), pairs in sorted(bucketed.items()):
        print(f"   [{bucket_min:.1f}-{bucket_max:.1f}): {len(pairs):,} pairs")
    
    return bucketed

def get_on_air_assets_from_snowflake(asset_ids):
    """Get set of (asset_id, source) tuples that are on-air from Snowflake"""
    if not asset_ids:
        return set()
    
    print(f"📊 Checking on-air status for {len(asset_ids)} assets...")
    
    snowflake = SnowflakeConnector()
    
    asset_list = "', '".join(str(aid) for aid in asset_ids)
    
    # artlist = product_indicator 1, motionarray = product_indicator 3
    query = f"""
    SELECT DISTINCT 
        ASSET_ID,
        CASE 
            WHEN PRODUCT_INDICATOR = 1 THEN 'artlist'
            WHEN PRODUCT_INDICATOR = 3 THEN 'motionarray'
            ELSE 'unknown'
        END as SOURCE
    FROM BI_PROD.dwh.DIM_ASSETS
    WHERE ASSET_ID IN ('{asset_list}')
        AND LAST_STATUS_GROUP = 'ON_AIR'
        AND PRODUCT_INDICATOR IN (1, 3)
        AND ASSET_TYPE = 'Music'
    """
    
    cursor = snowflake.execute_query(query)
    
    on_air_assets = set()
    source_breakdown = defaultdict(int)
    for row in cursor:
        asset_id = str(row[0])
        source = row[1]
        on_air_assets.add((asset_id, source))
        source_breakdown[source] += 1
    
    cursor.close()
    snowflake.close()
    
    print(f"   ✅ Found {len(on_air_assets)} on-air asset-source combinations")
    print(f"      Breakdown: {dict(source_breakdown)}")
    
    # Show first 5 examples
    if on_air_assets:
        print(f"      Sample on-air assets: {list(on_air_assets)[:5]}")
    
    return on_air_assets

def get_fingerprints_from_snowflake(asset_ids):
    """Get fingerprints for a list of asset IDs from Snowflake"""
    if not asset_ids:
        return {}
    
    print(f"📊 Fetching fingerprints from Snowflake for {len(asset_ids)} assets...")
    
    snowflake = SnowflakeConnector()
    
    # Create a mapping: (asset_id, source, format) -> fingerprint
    asset_list = "', '".join(str(aid) for aid in asset_ids)
    
    query = f"""
    SELECT 
        ASSET_ID,
        SOURCE,
        FORMAT,
        FINGERPRINT
    FROM AI_DATA.AUDIO_FINGERPRINT
    WHERE ASSET_ID IN ('{asset_list}')
        AND PROCESSING_STATUS = 'SUCCESS'
        AND FINGERPRINT IS NOT NULL
    ORDER BY ASSET_ID, SOURCE, FORMAT
    """
    
    cursor = snowflake.execute_query(query)
    
    fingerprints = {}
    for row in cursor:
        asset_id = str(row[0])
        source = row[1]
        format = row[2]
        fingerprint = row[3]
        
        # Use a composite key
        key = (asset_id, source, format)
        fingerprints[key] = fingerprint
    
    cursor.close()
    snowflake.close()
    
    print(f"   ✅ Retrieved {len(fingerprints)} fingerprints")
    
    return fingerprints

def sample_and_download(bucketed, samples_per_bucket, output_base_dir):
    """Sample random pairs from each bucket and download them"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(output_base_dir, f"similarity_samples_{timestamp}")
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\n📁 Downloading to: {output_dir}\n")
    
    # First, collect all sampled pairs and get their asset IDs
    all_sampled_pairs = []
    all_asset_ids = set()
    
    for (bucket_min, bucket_max), pairs in sorted(bucketed.items()):
        if pairs:
            sample_count = min(samples_per_bucket, len(pairs))
            sampled_pairs = random.sample(pairs, sample_count)
            all_sampled_pairs.append(((bucket_min, bucket_max), sampled_pairs))
            
            # Collect asset IDs
            for pair in sampled_pairs:
                all_asset_ids.add(pair['asset_id_1'])
                all_asset_ids.add(pair['asset_id_2'])
    
    # Fetch all fingerprints at once
    fingerprints = get_fingerprints_from_snowflake(list(all_asset_ids))
    print()
    
    summary = {
        "timestamp": timestamp,
        "samples_per_bucket": samples_per_bucket,
        "buckets": []
    }
    
    for (bucket_min, bucket_max), sampled_pairs in all_sampled_pairs:
        sample_count = len(sampled_pairs)
        total_in_bucket = len(bucketed[(bucket_min, bucket_max)])
        
        print(f"🎲 [{bucket_min:.1f}-{bucket_max:.1f}): Sampling {sample_count} random pairs from {total_in_bucket:,} available")
        
        # Create bucket directory
        bucket_name = f"similarity_{bucket_min:.1f}-{bucket_max:.1f}"
        bucket_dir = os.path.join(output_dir, bucket_name)
        os.makedirs(bucket_dir, exist_ok=True)
        
        bucket_info = {
            "range": [bucket_min, bucket_max],
            "total_pairs": len(pairs),
            "sampled": sample_count,
            "pairs": []
        }
        
        # Download each pair
        for i, pair in enumerate(sampled_pairs, 1):
            pair_dir = os.path.join(bucket_dir, f"pair_{i}")
            os.makedirs(pair_dir, exist_ok=True)
            
            print(f"   📥 Pair {i}/{sample_count}: Assets {pair['asset_id_1']} & {pair['asset_id_2']} (similarity: {pair['similarity']:.3f})")
            
            # Get fingerprints for this pair
            fp_key_1 = (pair['asset_id_1'], pair['source_1'], pair['format_1'])
            fp_key_2 = (pair['asset_id_2'], pair['source_2'], pair['format_2'])
            
            pair_info = {
                "pair_number": i,
                "asset_id_1": pair['asset_id_1'],
                "asset_id_2": pair['asset_id_2'],
                "similarity": pair['similarity'],
                "fingerprint_1": fingerprints.get(fp_key_1, None),
                "fingerprint_2": fingerprints.get(fp_key_2, None),
                "files": []
            }
            
            # Download file 1
            file1_name = f"asset_{pair['asset_id_1']}_{pair['source_1']}.{pair['format_1']}"
            file1_path = os.path.join(pair_dir, file1_name)
            
            print(f"      Downloading {file1_name}...")
            url1 = get_download_url_from_api(pair['file_key_1'], pair['source_1'])
            if url1 and download_file(url1, file1_path):
                print(f"      ✅ {file1_name}")
                pair_info['files'].append({
                    "asset_id": pair['asset_id_1'],
                    "filename": file1_name,
                    "source": pair['source_1'],
                    "format": pair['format_1'],
                    "duration": pair['duration_1']
                })
            else:
                print(f"      ❌ Failed to download {file1_name}")
            
            # Download file 2
            file2_name = f"asset_{pair['asset_id_2']}_{pair['source_2']}.{pair['format_2']}"
            file2_path = os.path.join(pair_dir, file2_name)
            
            print(f"      Downloading {file2_name}...")
            url2 = get_download_url_from_api(pair['file_key_2'], pair['source_2'])
            if url2 and download_file(url2, file2_path):
                print(f"      ✅ {file2_name}")
                pair_info['files'].append({
                    "asset_id": pair['asset_id_2'],
                    "filename": file2_name,
                    "source": pair['source_2'],
                    "format": pair['format_2'],
                    "duration": pair['duration_2']
                })
            else:
                print(f"      ❌ Failed to download {file2_name}")
            
            # Save pair info
            with open(os.path.join(pair_dir, "info.json"), 'w') as f:
                json.dump(pair_info, f, indent=2)
            
            bucket_info['pairs'].append(pair_info)
        
        summary['buckets'].append(bucket_info)
        print()
    
    # Save summary
    with open(os.path.join(output_dir, "summary.json"), 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"✅ Download complete!")
    print(f"📁 Files saved to: {output_dir}")

def main():
    parser = argparse.ArgumentParser(
        description='Sample and download audio pairs from different similarity ranges',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download 5 random pairs from each 0.1-wide bucket
  python sample_similarity_buckets.py
  
  # Download 10 pairs per bucket
  python sample_similarity_buckets.py --samples 10
  
  # Only sample same-format pairs (mp3→mp3, wav→wav)
  python sample_similarity_buckets.py --same-format-only
  
  # Only sample cross-source pairs (artlist↔motionarray)
  python sample_similarity_buckets.py --cross-source-only
  
  # Only sample same-source pairs (artlist↔artlist, motionarray↔motionarray)
  python sample_similarity_buckets.py --same-source-only
  
  # Only sample on-air assets
  python sample_similarity_buckets.py --on-air-only
  
  # Combine filters: same-format AND same-source AND on-air
  python sample_similarity_buckets.py --same-format-only --same-source-only --on-air-only
  
  # Custom buckets (every 0.05)
  python sample_similarity_buckets.py --buckets 0.0,0.05,0.1,0.15,0.2,0.25,0.3,0.5,1.0
        """
    )
    
    parser.add_argument(
        '--samples',
        type=int,
        default=5,
        help='Number of random pairs to sample from each bucket (default: 5)'
    )
    
    parser.add_argument(
        '--buckets',
        type=str,
        default='0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,1.1',
        help='Comma-separated similarity thresholds (default: 0.0,0.1,0.2,...,1.0,1.1)'
    )
    
    parser.add_argument(
        '--input',
        type=str,
        default='evaluation/duplicate_results_all_20251017_124125.jsonl',
        help='Input JSONL file (default: duplicate_results_all_20251017_124125.jsonl)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='evaluation',
        help='Output base directory (default: evaluation)'
    )
    
    parser.add_argument(
        '--same-format-only',
        action='store_true',
        help='Only sample pairs with the same format (mp3→mp3, wav→wav, etc.)'
    )
    
    parser.add_argument(
        '--cross-source-only',
        action='store_true',
        help='Only sample pairs from different sources (artlist↔motionarray)'
    )
    
    parser.add_argument(
        '--same-source-only',
        action='store_true',
        help='Only sample pairs from the same source (artlist↔artlist, motionarray↔motionarray)'
    )
    
    parser.add_argument(
        '--on-air-only',
        action='store_true',
        help='Only sample pairs where both assets have LAST_STATUS_GROUP=\'ON_AIR\''
    )
    
    args = parser.parse_args()
    
    # Validate mutually exclusive options
    if args.cross_source_only and args.same_source_only:
        parser.error("--cross-source-only and --same-source-only are mutually exclusive")
    
    # Parse bucket thresholds
    bucket_thresholds = [float(x) for x in args.buckets.split(',')]
    bucket_thresholds.sort()
    
    print("🎵 Similarity Bucket Sampler")
    print("=" * 50)
    print(f"Input file: {args.input}")
    print(f"Samples per bucket: {args.samples}")
    print(f"Bucket thresholds: {bucket_thresholds}")
    if args.same_format_only:
        print(f"Filter: Same format only ✓")
    if args.cross_source_only:
        print(f"Filter: Cross-source only ✓")
    if args.same_source_only:
        print(f"Filter: Same-source only ✓")
    if args.on_air_only:
        print(f"Filter: On-air assets only ✓")
    print()
    
    # Load and bucket
    bucketed = load_and_bucket_pairs(args.input, bucket_thresholds, args.same_format_only, args.cross_source_only, args.same_source_only, args.on_air_only)
    
    # Sample and download
    sample_and_download(bucketed, args.samples, args.output)

if __name__ == "__main__":
    main()

