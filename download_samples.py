#!/usr/bin/env python3
"""
Simple download script for missed duplicate samples.
Uses the EXACT same API method as audio_fingerprint_processor.py
"""

import json
import requests
from pathlib import Path
import logging
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_download_url_from_api(file_key: str, source: str) -> Optional[str]:
    """Get signed download URL from Artlist/MotionArray API - EXACT copy from audio_fingerprint_processor.py"""
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
        
        logger.warning(f"⚠️  No download URL found in API response for {file_key}")
        return None
        
    except Exception as e:
        logger.warning(f"❌ API request failed for {file_key}: {e}")
        return None

def download_file(url: str, output_path: Path):
    """Download file from URL"""
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    
    # Validate file
    size = output_path.stat().st_size
    if size < 10240:  # Less than 10KB is suspicious
        raise Exception(f"File too small: {size} bytes")
    
    return size

def main():
    # Load missed duplicates
    with open('missed_duplicates_top10.json', 'r') as f:
        missed = json.load(f)
    
    # Create output directory
    output_dir = Path('missed_duplicates_samples')
    output_dir.mkdir(exist_ok=True)
    
    logger.info(f"Downloading {len(missed)} duplicate pairs...")
    logger.info(f"Output directory: {output_dir}\n")
    
    for i, dup in enumerate(missed[:5], 1):  # Download first 5 pairs only
        logger.info(f"{'='*70}")
        logger.info(f"PAIR {i}/5")
        logger.info(f"Similarity: {dup['similarity']:.3f} | Duration diff: {dup['duration_diff']:.3f}s")
        
        # Create pair directory
        pair_dir = output_dir / f"pair{i:02d}_sim{dup['similarity']:.3f}"
        pair_dir.mkdir(exist_ok=True)
        
        # Save metadata
        with open(pair_dir / 'info.json', 'w') as f:
            json.dump(dup, f, indent=2)
        
        # Download file 1
        try:
            logger.info(f"\nFile 1: {dup['file_key_1']} ({dup['source_1']}, {dup['format_1']})")
            url1 = get_download_url_from_api(dup['file_key_1'], dup['source_1'])
            if url1:
                file1_path = pair_dir / f"file1_{dup['source_1']}.{dup['format_1']}"
                size1 = download_file(url1, file1_path)
                logger.info(f"  ✅ Downloaded: {size1/1024/1024:.1f} MB")
            else:
                logger.error(f"  ❌ Failed to get download URL")
        except Exception as e:
            logger.error(f"  ❌ Failed: {e}")
        
        # Download file 2
        try:
            logger.info(f"\nFile 2: {dup['file_key_2']} ({dup['source_2']}, {dup['format_2']})")
            url2 = get_download_url_from_api(dup['file_key_2'], dup['source_2'])
            if url2:
                file2_path = pair_dir / f"file2_{dup['source_2']}.{dup['format_2']}"
                size2 = download_file(url2, file2_path)
                logger.info(f"  ✅ Downloaded: {size2/1024/1024:.1f} MB")
            else:
                logger.error(f"  ❌ Failed to get download URL")
        except Exception as e:
            logger.error(f"  ❌ Failed: {e}")
    
    logger.info(f"\n{'='*70}")
    logger.info(f"✅ Done! Files saved to: {output_dir.absolute()}")
    logger.info(f"\nYou can now listen to the pairs and decide if they're truly duplicates.")

if __name__ == '__main__':
    main()

