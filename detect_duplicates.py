#!/usr/bin/env python3
"""
Prepare Duplicate Table - Transform duplicate groups into per-song format for Snowflake
"""

import os
import sys
import csv
import json
import argparse
from datetime import datetime
from collections import defaultdict
from snowflake_utils import SnowflakeConnector

# Source name to product_indicator mapping
SOURCE_TO_INDICATOR = {
    'artlist': 1,
    'motionarray': 3,
    # Add more mappings as needed
}

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
            'asset_id': row[0], 
            'file_key': row[1], 
            'format': row[2],
            'duration': float(row[3]), 
            'fingerprint': row[4], 
            'source': row[5]
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

def transform_to_per_song_format(duplicates):
    """
    Transform duplicate groups into per-song format.
    Each song gets its own row with all its duplicates listed.
    
    Returns:
        List of dicts with: song_id, product_indicator, asset_type, duplicates
    """
    print("Transforming to per-song format...")
    
    result_rows = []
    seen_songs = set()  # Track (song_id, product_indicator) to avoid duplicates
    
    for fp_str, files in duplicates.items():
        # For each file in the group, create a row where duplicates = all OTHER files
        for file in files:
            song_id = file['asset_id']
            source = file['source'].lower()
            product_indicator = SOURCE_TO_INDICATOR.get(source, 0)
            
            # Skip if we've already processed this (song_id, product_indicator)
            song_key = (song_id, product_indicator)
            if song_key in seen_songs:
                continue
            seen_songs.add(song_key)
            
            # Get all other files in the group (excluding current file)
            other_files = [f for f in files if f['asset_id'] != file['asset_id'] or f['source'] != file['source']]
            
            # Format duplicates as list of {product_indicator, song_id} objects
            # Note: song_id is string in the new format
            duplicates_list = []
            for other_file in other_files:
                other_source = other_file['source'].lower()
                other_indicator = SOURCE_TO_INDICATOR.get(other_source, 0)
                duplicates_list.append({
                    'product_indicator': other_indicator,
                    'song_id': str(other_file['asset_id'])  # String format as shown in picture
                })
            
            result_rows.append({
                'song_id': song_id,
                'product_indicator': product_indicator,
                'asset_type': 'music',
                'duplicates': duplicates_list
            })
    
    print(f"Created {len(result_rows):,} rows (deduplicated)")
    return result_rows

def export_to_csv(rows, filename=None):
    """Export results to CSV file"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        evaluation_dir = "evaluation"
        os.makedirs(evaluation_dir, exist_ok=True)
        filename = os.path.join(evaluation_dir, f"duplicate_table_{timestamp}.csv")
    
    print(f"Exporting to {filename}...")
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow(['song_id', 'product_indicator', 'asset_type', 'duplicates'])
        
        # Data rows
        for row in rows:
            # Format duplicates as proper JSON array
            duplicates_json = json.dumps(row['duplicates'])
            
            writer.writerow([
                row['song_id'],
                row['product_indicator'],
                row['asset_type'],
                duplicates_json
            ])
    
    print(f"✅ Exported {len(rows):,} rows to {filename}")
    return filename

def export_to_jsonl(rows, filename=None):
    """Export results to JSONL file (easier for Snowflake bulk insert)"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        evaluation_dir = "evaluation"
        os.makedirs(evaluation_dir, exist_ok=True)
        filename = os.path.join(evaluation_dir, f"duplicate_table_{timestamp}.jsonl")
    
    print(f"Exporting to {filename}...")
    
    with open(filename, 'w') as f:
        for row in rows:
            f.write(json.dumps(row) + '\n')
    
    print(f"✅ Exported {len(rows):,} rows to {filename}")
    return filename

def create_duplicate_table():
    """Create the duplicate table in Snowflake if it doesn't exist"""
    snowflake = SnowflakeConnector()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS BI_PROD.AI_DATA.DUPLICATED_ASSETS (
        SONG_ID INTEGER NOT NULL,
        PRODUCT_INDICATOR INTEGER NOT NULL,
        ASSET_TYPE VARCHAR(50) NOT NULL,
        DUPLICATES ARRAY,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (SONG_ID, PRODUCT_INDICATOR)
    );
    """
    
    print("Creating duplicate table in BI_PROD.AI_DATA.DUPLICATED_ASSETS...")
    cursor = snowflake.execute_query(create_table_query)
    cursor.close()
    snowflake.close()
    print("✅ Table created/verified")

def write_to_snowflake(rows):
    """Write duplicate data to Snowflake"""
    print(f"Writing {len(rows):,} rows to BI_PROD.AI_DATA.DUPLICATED_ASSETS...")
    
    snowflake = SnowflakeConnector()
    
    # Clear existing data
    print("  Clearing existing data...")
    delete_query = "DELETE FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS"
    cursor = snowflake.execute_query(delete_query)
    cursor.close()
    
    # Insert new data in batches
    batch_size = 500  # Reduced batch size for complex JSON
    total_batches = (len(rows) + batch_size - 1) // batch_size
    
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        print(f"  Inserting batch {batch_num}/{total_batches}...")
        
        # Build insert query using SELECT with PARSE_JSON
        select_parts = []
        for row in batch:
            duplicates_json = json.dumps(row['duplicates']).replace("'", "''")
            select_parts.append(
                f"SELECT {row['song_id']} AS SONG_ID, {row['product_indicator']} AS PRODUCT_INDICATOR, "
                f"'{row['asset_type']}' AS ASSET_TYPE, PARSE_JSON('{duplicates_json}') AS DUPLICATES"
            )
        
        insert_query = f"""
        INSERT INTO BI_PROD.AI_DATA.DUPLICATED_ASSETS (SONG_ID, PRODUCT_INDICATOR, ASSET_TYPE, DUPLICATES)
        {' UNION ALL '.join(select_parts)}
        """
        
        cursor = snowflake.execute_query(insert_query)
        cursor.close()
    
    snowflake.close()
    print(f"✅ Successfully wrote {len(rows):,} rows to Snowflake")

def show_sample(rows, n=5):
    """Show sample rows for verification"""
    print(f"\n📋 Sample rows (first {n}):")
    for i, row in enumerate(rows[:n], 1):
        duplicates_preview = row['duplicates'][:3]
        duplicates_str = json.dumps(duplicates_preview)
        if len(row['duplicates']) > 3:
            duplicates_str = duplicates_str[:-1] + f", ... (+{len(row['duplicates'])-3} more)]"
        
        print(f"  {i}. Song {row['song_id']} (product {row['product_indicator']}, {row['asset_type']}) → {duplicates_str}")

def main(write_to_snowflake_flag=False, skip_confirmation=False):
    """Main workflow"""
    print("🔄 Preparing Duplicate Table")
    print("=" * 60)
    
    # Load and find duplicates
    fingerprints = load_fingerprints()
    duplicates = find_exact_duplicates(fingerprints)
    
    # Transform to per-song format
    rows = transform_to_per_song_format(duplicates)
    
    # Show sample
    show_sample(rows)
    
    # Export to files for review
    csv_file = export_to_csv(rows)
    jsonl_file = export_to_jsonl(rows)
    
    print(f"\n✅ Done! Files created:")
    print(f"   CSV:  {csv_file}")
    print(f"   JSONL: {jsonl_file}")
    print(f"   Total rows: {len(rows):,}")
    
    # Write to Snowflake if flag is enabled
    if write_to_snowflake_flag:
        print(f"\n🚀 Writing to Snowflake (BI_PROD.AI_DATA.DUPLICATED_ASSETS)...")
        
        if skip_confirmation:
            print("⚠️  Skipping confirmation (automated run)")
            create_duplicate_table()
            write_to_snowflake(rows)
        else:
            confirmation = input("⚠️  This will DELETE existing data and write new data. Continue? [y/N]: ")
            if confirmation.lower() == 'y':
                create_duplicate_table()
                write_to_snowflake(rows)
            else:
                print("❌ Snowflake write cancelled")
    else:
        print(f"\n💡 To write to Snowflake, run with --write flag:")
        print(f"   python prepare_duplicate_table.py --write")
    
    return rows

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Prepare duplicate table for Snowflake')
    parser.add_argument('--write', action='store_true', 
                        help='Write data to BI_PROD.AI_DATA.DUPLICATED_ASSETS (requires confirmation)')
    parser.add_argument('--yes', action='store_true',
                        help='Skip confirmation prompt')
    args = parser.parse_args()
    
    rows = main(write_to_snowflake_flag=args.write, skip_confirmation=args.yes)

