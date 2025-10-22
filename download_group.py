#!/usr/bin/env python3
"""
Simple Group Downloader - Download a specific duplicate group by ID
Usage: python download_group.py <group_id>
"""

import sys
import csv
import os
import json
import requests
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
        
        print(f"   ⚠️  No download URL found for {file_key}")
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
            print(f"   ❌ Downloaded file is empty or too small")
            return False
            
    except Exception as e:
        print(f"   ❌ Download error: {e}")
        return False

def get_group_from_csv(group_id, csv_file="evaluation/duplicate_groups_20251021_123057.csv"):
    """Get group info from CSV file"""
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found: {csv_file}")
        return None
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if int(row['group_id']) == group_id:
                return {
                    'group_id': int(row['group_id']),
                    'copy_count': int(row['copy_count']),
                    'is_cross_source': row['is_cross_source'].lower() == 'true',
                    'sources': row['sources'].split(', '),
                    'assets': [asset.strip() for asset in row['assets'].split(', ')],
                    'fingerprint_preview': row['fingerprint_preview']
                }
    
    print(f"❌ Group {group_id} not found in CSV")
    return None

def get_file_info_from_snowflake(asset_ids):
    """Get file metadata from Snowflake"""
    snowflake = SnowflakeConnector()
    
    asset_list = "', '".join(str(aid) for aid in asset_ids)
    
    query = f"""
    SELECT 
        ASSET_ID,
        FILE_KEY,
        SOURCE,
        FORMAT,
        DURATION
    FROM AI_DATA.AUDIO_FINGERPRINT
    WHERE ASSET_ID IN ('{asset_list}')
        AND PROCESSING_STATUS = 'SUCCESS'
        AND FINGERPRINT IS NOT NULL
    ORDER BY ASSET_ID, SOURCE, FORMAT
    """
    
    cursor = snowflake.execute_query(query)
    
    files = []
    for row in cursor:
        files.append({
            'asset_id': row[0],
            'file_key': row[1],
            'source': row[2],
            'format': row[3],
            'duration': float(row[4])
        })
    
    cursor.close()
    snowflake.close()
    
    return files

def download_group(group_id):
    """Download all files for a specific group"""
    print(f"🔍 Looking for group {group_id}...")
    
    # Get group info from CSV
    group = get_group_from_csv(group_id)
    if not group:
        return False
    
    print(f"✅ Found group {group_id}:")
    print(f"   Copies: {group['copy_count']}")
    print(f"   Cross-source: {group['is_cross_source']}")
    print(f"   Sources: {', '.join(group['sources'])}")
    print(f"   Assets: {', '.join(group['assets'])}")
    
    # Get file info from Snowflake
    print(f"\n🔍 Getting file info from Snowflake...")
    file_info = get_file_info_from_snowflake(group['assets'])
    
    if not file_info:
        print(f"❌ No file info found for group {group_id}")
        return False
    
    # Create asset_id -> file_info mapping
    asset_to_files = {}
    for file in file_info:
        asset_id = str(file['asset_id'])
        if asset_id not in asset_to_files:
            asset_to_files[asset_id] = []
        asset_to_files[asset_id].append(file)
    
    # Create download directory in evaluation folder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    evaluation_dir = "evaluation"
    download_dir = os.path.join(evaluation_dir, f"group_{group_id}_download_{timestamp}")
    os.makedirs(download_dir, exist_ok=True)
    
    print(f"\n📁 Downloading to: {download_dir}")
    
    # Download files
    downloaded_files = []
    success = True
    
    for asset_id in group['assets']:
        asset_files = asset_to_files.get(str(asset_id), [])
        if not asset_files:
            print(f"   ⚠️  No file info found for asset {asset_id}")
            continue
        
        # Take the first available file for this asset
        file_info = asset_files[0]
        
        print(f"\n📥 Asset {asset_id} ({file_info['source']}, {file_info['format']}, {file_info['duration']:.1f}s)")
        
        # Get download URL
        download_url = get_download_url_from_api(file_info['file_key'], file_info['source'])
        if not download_url:
            print(f"   ❌ Could not get download URL")
            success = False
            continue
        
        # Create filename
        filename = f"asset_{asset_id}_{file_info['source']}.{file_info['format']}"
        output_path = os.path.join(download_dir, filename)
        
        print(f"   📁 Downloading {filename}...")
        
        if download_file(download_url, output_path):
            print(f"   ✅ Downloaded: {filename}")
            downloaded_files.append({
                "asset_id": asset_id,
                "filename": filename,
                "source": file_info['source'],
                "format": file_info['format'],
                "duration": file_info['duration'],
                "file_path": output_path
            })
        else:
            print(f"   ❌ Failed to download: {filename}")
            success = False
    
    # Create info file
    group_info = {
        "group_id": group['group_id'],
        "copy_count": group['copy_count'],
        "is_cross_source": group['is_cross_source'],
        "sources": group['sources'],
        "assets": group['assets'],
        "fingerprint_preview": group['fingerprint_preview'],
        "downloaded_files": downloaded_files,
        "download_success": success,
        "downloaded_at": datetime.now().isoformat()
    }
    
    with open(os.path.join(download_dir, "info.json"), 'w') as f:
        json.dump(group_info, f, indent=2)
    
    print(f"\n{'✅' if success else '⚠️'} Download {'complete' if success else 'completed with errors'}!")
    print(f"📁 Files saved to: {download_dir}")
    print(f"📊 Downloaded {len(downloaded_files)}/{len(group['assets'])} files")
    
    return success

def main():
    if len(sys.argv) != 2:
        print("Usage: python download_group.py <group_id>")
        print("\nExample:")
        print("  python download_group.py 1")
        print("  python download_group.py 42")
        print("\nThis will download all assets from the specified duplicate group.")
        return 1
    
    try:
        group_id = int(sys.argv[1])
    except ValueError:
        print("❌ Group ID must be a number")
        return 1
    
    print(f"🎵 Group Downloader")
    print(f"=" * 30)
    
    success = download_group(group_id)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
