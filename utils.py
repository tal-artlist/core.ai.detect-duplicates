#!/usr/bin/env python3
"""
General utility functions for the bulk downloader.
"""

import json
import logging
import os
from pathlib import Path
from typing import List, Dict, Any
from urllib.parse import urlparse
import requests

logger = logging.getLogger(__name__)


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logging for the application."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def create_directories(*dirs: Path) -> None:
    """Create directories if they don't exist."""
    for directory in dirs:
        directory.mkdir(parents=True, exist_ok=True)


def extract_filename_from_url(url: str, fallback_key: str) -> str:
    """
    Extract filename from URL or use fallback key.
    
    Args:
        url: The URL to extract filename from
        fallback_key: Fallback key to use if no filename found
        
    Returns:
        Extracted or generated filename
    """
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    return filename if filename else f"{fallback_key}.bin"


def download_file(url: str, file_path: Path, timeout: int = 60) -> bool:
    """
    Download a file from URL to specified path.
    
    Args:
        url: URL to download from
        file_path: Path to save the file
        timeout: Request timeout in seconds
        
    Returns:
        True if download successful, False otherwise
    """
    try:
        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()
        
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        logger.info(f"Successfully downloaded: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download file from {url}: {e}")
        return False


def remove_duplicates_preserve_order(items: List[str]) -> List[str]:
    """
    Remove duplicates from list while preserving order.
    
    Args:
        items: List of items that may contain duplicates
        
    Returns:
        List with duplicates removed, order preserved
    """
    unique_items = []
    seen = set()
    
    for item in items:
        if item not in seen:
            unique_items.append(item)
            seen.add(item)
    
    return unique_items


def extract_keys_from_snowflake_data(data: List[Dict[str, Any]]) -> List[str]:
    """
    Extract file keys from Snowflake query results.
    
    The KEY_FORMAT_PAIRS column contains JSON like:
    [
      {
        "file_key": "PRD-1963345-C0asqHM3CCzrWgOz-original",
        "format": "WAV"
      },
      ...
    ]
    
    Args:
        data: List of dictionaries from Snowflake query
        
    Returns:
        Flattened list of file keys
    """
    keys = []
    total_pairs = 0
    
    for asset in data:
        asset_id = asset.get('ASSET_ID', 'unknown')
        try:
            # Primary method: Extract from KEY_FORMAT_PAIRS
            if 'KEY_FORMAT_PAIRS' in asset and asset['KEY_FORMAT_PAIRS']:
                key_format_pairs_str = asset['KEY_FORMAT_PAIRS']
                
                if isinstance(key_format_pairs_str, str):
                    # Parse JSON string
                    key_format_pairs = json.loads(key_format_pairs_str)
                    
                    if isinstance(key_format_pairs, list):
                        for pair in key_format_pairs:
                            if isinstance(pair, dict) and 'file_key' in pair:
                                file_key = pair['file_key']
                                format_type = pair.get('format', 'unknown')
                                keys.append(file_key)
                                total_pairs += 1
                                logger.debug(f"Asset {asset_id}: {file_key} ({format_type})")
                    else:
                        logger.warning(f"Asset {asset_id}: KEY_FORMAT_PAIRS is not a list after parsing")
                
                elif isinstance(key_format_pairs_str, list):
                    # Already parsed as list
                    for pair in key_format_pairs_str:
                        if isinstance(pair, dict) and 'file_key' in pair:
                            file_key = pair['file_key']
                            format_type = pair.get('format', 'unknown')
                            keys.append(file_key)
                            total_pairs += 1
                            logger.debug(f"Asset {asset_id}: {file_key} ({format_type})")
            
            # Fallback method: Extract from FILE_KEYS array
            elif 'FILE_KEYS' in asset and asset['FILE_KEYS']:
                logger.info(f"Asset {asset_id}: Using fallback FILE_KEYS method")
                file_keys_str = asset['FILE_KEYS']
                
                if isinstance(file_keys_str, str):
                    # Parse JSON string
                    file_keys = json.loads(file_keys_str)
                    if isinstance(file_keys, list):
                        keys.extend(file_keys)
                        total_pairs += len(file_keys)
                elif isinstance(file_keys_str, list):
                    # Already parsed
                    keys.extend(file_keys_str)
                    total_pairs += len(file_keys_str)
            
            else:
                logger.warning(f"Asset {asset_id}: No KEY_FORMAT_PAIRS or FILE_KEYS found")
                    
        except json.JSONDecodeError as e:
            logger.error(f"Asset {asset_id}: Failed to parse JSON - {e}")
            logger.error(f"Raw data: {asset.get('KEY_FORMAT_PAIRS', 'N/A')[:200]}...")
            continue
        except Exception as e:
            logger.error(f"Asset {asset_id}: Unexpected error extracting keys - {e}")
            continue
    
    logger.info(f"Successfully extracted {len(keys)} file keys from {len(data)} assets ({total_pairs} total key-format pairs)")
    
    # Remove duplicates while preserving order
    unique_keys = remove_duplicates_preserve_order(keys)
    
    if len(unique_keys) != len(keys):
        logger.info(f"Removed {len(keys) - len(unique_keys)} duplicate keys")
    
    return unique_keys


def create_download_summary(results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a summary of download results from both platforms.
    
    Args:
        results: Dictionary containing artlist and motionarray results
        
    Returns:
        Summary dictionary with aggregated statistics
    """
    artlist_success = results.get('artlist', {}).get('success', False)
    motionarray_success = results.get('motionarray', {}).get('success', False)
    
    artlist_downloads = results.get('artlist', {}).get('downloads_successful', 0)
    motionarray_downloads = results.get('motionarray', {}).get('downloads_successful', 0)
    
    return {
        'overall_success': artlist_success or motionarray_success,
        'artlist_success': artlist_success,
        'motionarray_success': motionarray_success,
        'total_downloads': artlist_downloads + motionarray_downloads,
        'artlist_downloads': artlist_downloads,
        'motionarray_downloads': motionarray_downloads
    }
