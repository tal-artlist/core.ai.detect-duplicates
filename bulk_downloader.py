#!/usr/bin/env python3
"""
Bulk downloader for Artlist and MotionArray content artifacts.

This script provides functions to:
1. Obtain keys from Snowflake queries
2. Bulk download artifacts using the platform APIs
3. Handle download responses and file management
"""

import json
import logging
import sqlite3
from pathlib import Path
from typing import List, Dict, Any
import requests

from utils import (
    setup_logging, 
    create_directories, 
    extract_filename_from_url, 
    download_file,
    extract_keys_from_snowflake_data,
    create_download_summary
)
from snowflake_utils import (
    get_artlist_keys_from_snowflake,
    get_motionarray_keys_from_snowflake
)

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


class BulkDownloader:
    """Main class for handling bulk downloads from Artlist and MotionArray APIs."""
    
    def __init__(self, download_dir: str = "downloads"):
        """
        Initialize the bulk downloader.
        
        Args:
            download_dir: Directory to save downloaded files
        """
        # API configurations
        self.api_url = "https://oapi-int.artlist.io/v1/content/bulkDownloadArtifacts"
        self.headers = {
            'service-host': 'core.content.cms.api',
            'Content-Type': 'application/json'
        }
        
        # Setup directories
        self.download_dir = Path(download_dir)
        self.artlist_dir = self.download_dir / "artlist"
        self.motionarray_dir = self.download_dir / "motionarray"
        
        create_directories(self.download_dir, self.artlist_dir, self.motionarray_dir)
    
    def get_keys_from_sql(self, db_path: str, query: str) -> List[str]:
        """
        Execute SQL query to obtain download keys from SQLite database.
        
        Args:
            db_path: Path to the SQLite database file
            query: SQL query to execute (should return keys in first column)
            
        Returns:
            List of keys obtained from the query
            
        Raises:
            sqlite3.Error: If database operation fails
        """
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                
                # Extract keys from first column of results
                keys = [str(row[0]) for row in results if row[0] is not None]
                
                logger.info(f"Retrieved {len(keys)} keys from SQL query")
                return keys
                
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing SQL query: {e}")
            raise
    
    def bulk_download_request(self, keys: List[str], platform: str = "artlist") -> Dict[str, Any]:
        """
        Make bulk download request to specified platform API.
        
        Args:
            keys: List of artifact keys to download
            platform: Platform to download from ("artlist" or "motionarray")
            
        Returns:
            API response as dictionary
            
        Raises:
            requests.RequestException: If API request fails
        """
        payload = {"keys": keys}
        
        try:
            logger.info(f"Making {platform} bulk download request for {len(keys)} keys")
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"{platform} bulk download request successful")
            return result
            
        except requests.exceptions.Timeout:
            logger.error("Request timed out")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response content: {e.response.text}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise
    
    def download_files(self, download_urls: Dict[str, str], platform: str = "artlist") -> Dict[str, bool]:
        """
        Download files from provided URLs.
        
        Args:
            download_urls: Dictionary mapping keys to download URLs
            platform: Platform to determine download directory ("artlist" or "motionarray")
            
        Returns:
            Dictionary mapping keys to download success status
        """
        results = {}
        
        # Choose appropriate download directory
        target_dir = self.artlist_dir if platform.lower() == "artlist" else self.motionarray_dir
        
        for key, url in download_urls.items():
            try:
                logger.info(f"Downloading {platform} file for key: {key}")
                
                # Get filename from URL or use key as fallback
                filename = extract_filename_from_url(url, key)
                file_path = target_dir / filename
                
                # Download file
                success = download_file(url, file_path)
                results[key] = success
                
            except Exception as e:
                logger.error(f"Failed to download file for key {key}: {e}")
                results[key] = False
        
        return results
    
    def process_bulk_download(self, keys: List[str], platform: str = "artlist") -> Dict[str, Any]:
        """
        Complete bulk download process: request URLs and download files.
        
        Args:
            keys: List of artifact keys to download
            platform: Platform to download from ("artlist" or "motionarray")
            
        Returns:
            Dictionary with download results and metadata
        """
        try:
            # Get download URLs from API
            api_response = self.bulk_download_request(keys, platform)
            
            # Extract download URLs from API response structure
            download_urls = {}
            if 'data' in api_response and 'downloadArtifactResponses' in api_response['data']:
                responses = api_response['data']['downloadArtifactResponses']
                for key, response in responses.items():
                    if isinstance(response, dict) and 'url' in response:
                        # Use the fileName as key if available, otherwise use the numeric key
                        file_key = response.get('fileName', f"file_{key}")
                        download_urls[file_key] = response['url']
            
            if not download_urls:
                logger.warning("No download URLs found in API response")
                return {
                    'success': False,
                    'message': 'No download URLs received',
                    'api_response': api_response,
                    'platform': platform
                }
            
            # Download files
            download_results = self.download_files(download_urls, platform)
            
            # Summary
            successful_downloads = sum(1 for success in download_results.values() if success)
            total_downloads = len(download_results)
            
            return {
                'success': successful_downloads > 0,
                'platform': platform,
                'total_requested': len(keys),
                'urls_received': len(download_urls),
                'downloads_attempted': total_downloads,
                'downloads_successful': successful_downloads,
                'download_results': download_results,
                'api_response': api_response
            }
            
        except Exception as e:
            logger.error(f"Bulk download process failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'platform': platform,
                'total_requested': len(keys)
            }
    
    def download_artlist_songs(self, limit: int = 100) -> Dict[str, Any]:
        """
        Download Artlist songs using Snowflake query.
        
        Args:
            limit: Maximum number of assets to download
            
        Returns:
            Dictionary with download results
        """
        try:
            logger.info(f"Starting Artlist download for {limit} songs")
            
            # Get data from Snowflake
            snowflake_data = get_artlist_keys_from_snowflake(limit)
            
            if not snowflake_data:
                return {
                    'success': False,
                    'platform': 'artlist',
                    'error': 'No data retrieved from Snowflake'
                }
            
            # Extract keys
            keys = extract_keys_from_snowflake_data(snowflake_data)
            
            if not keys:
                return {
                    'success': False,
                    'platform': 'artlist',
                    'error': 'No file keys found in Snowflake data'
                }
            
            logger.info(f"Extracted {len(keys)} file keys from {len(snowflake_data)} assets")
            
            # Process bulk download
            result = self.process_bulk_download(keys, "artlist")
            result['snowflake_assets'] = len(snowflake_data)
            result['extracted_keys'] = len(keys)
            
            return result
            
        except Exception as e:
            logger.error(f"Artlist download failed: {e}")
            return {
                'success': False,
                'platform': 'artlist',
                'error': str(e)
            }
    
    def download_motionarray_songs(self, limit: int = 100) -> Dict[str, Any]:
        """
        Download MotionArray songs using Snowflake query.
        
        Args:
            limit: Maximum number of assets to download
            
        Returns:
            Dictionary with download results
        """
        try:
            logger.info(f"Starting MotionArray download for {limit} songs")
            
            # Get data from Snowflake
            snowflake_data = get_motionarray_keys_from_snowflake(limit)
            
            if not snowflake_data:
                return {
                    'success': False,
                    'platform': 'motionarray',
                    'error': 'No data retrieved from Snowflake'
                }
            
            # Extract keys
            keys = extract_keys_from_snowflake_data(snowflake_data)
            
            if not keys:
                return {
                    'success': False,
                    'platform': 'motionarray',
                    'error': 'No file keys found in Snowflake data'
                }
            
            logger.info(f"Extracted {len(keys)} file keys from {len(snowflake_data)} assets")
            
            # Process bulk download
            result = self.process_bulk_download(keys, "motionarray")
            result['snowflake_assets'] = len(snowflake_data)
            result['extracted_keys'] = len(keys)
            
            return result
            
        except Exception as e:
            logger.error(f"MotionArray download failed: {e}")
            return {
                'success': False,
                'platform': 'motionarray',
                'error': str(e)
            }
    
    def download_both_platforms(self, artlist_limit: int = 100, motionarray_limit: int = 100) -> Dict[str, Any]:
        """
        Download songs from both Artlist and MotionArray platforms.
        
        Args:
            artlist_limit: Maximum number of Artlist assets to download
            motionarray_limit: Maximum number of MotionArray assets to download
            
        Returns:
            Dictionary with combined download results
        """
        logger.info(f"Starting downloads from both platforms: Artlist({artlist_limit}), MotionArray({motionarray_limit})")
        
        results = {
            'artlist': None,
            'motionarray': None,
            'summary': {}
        }
        
        # Download from Artlist
        try:
            results['artlist'] = self.download_artlist_songs(artlist_limit)
        except Exception as e:
            logger.error(f"Artlist download failed: {e}")
            results['artlist'] = {
                'success': False,
                'platform': 'artlist',
                'error': str(e)
            }
        
        # Download from MotionArray
        try:
            results['motionarray'] = self.download_motionarray_songs(motionarray_limit)
        except Exception as e:
            logger.error(f"MotionArray download failed: {e}")
            results['motionarray'] = {
                'success': False,
                'platform': 'motionarray',
                'error': str(e)
            }
        
        # Create summary
        results['summary'] = create_download_summary(results)
        
        logger.info(f"Download summary: Total={results['summary']['total_downloads']}, "
                   f"Artlist={results['summary']['artlist_downloads']}, "
                   f"MotionArray={results['summary']['motionarray_downloads']}")
        
        return results


def main():
    """Main function to run the bulk downloader."""
    downloader = BulkDownloader()
    
    print("=== BULK DOWNLOADER - DUAL PLATFORM ===")
    print("This script will download 100 songs from both Artlist and MotionArray")
    print("=" * 50)
    
    try:
        # Download from both platforms
        results = downloader.download_both_platforms(
            artlist_limit=100,
            motionarray_limit=100
        )
        
        # Print detailed results
        print("\n=== DOWNLOAD RESULTS ===")
        print(json.dumps(results['summary'], indent=2))
        
        # Print platform-specific details
        if results['artlist']:
            print(f"\n--- ARTLIST DETAILS ---")
            if results['artlist']['success']:
                print(f"✓ Success: {results['artlist']['downloads_successful']} files downloaded")
                print(f"  Assets from Snowflake: {results['artlist'].get('snowflake_assets', 'N/A')}")
                print(f"  Keys extracted: {results['artlist'].get('extracted_keys', 'N/A')}")
            else:
                print(f"✗ Failed: {results['artlist'].get('error', 'Unknown error')}")
        
        if results['motionarray']:
            print(f"\n--- MOTIONARRAY DETAILS ---")
            if results['motionarray']['success']:
                print(f"✓ Success: {results['motionarray']['downloads_successful']} files downloaded")
                print(f"  Assets from Snowflake: {results['motionarray'].get('snowflake_assets', 'N/A')}")
                print(f"  Keys extracted: {results['motionarray'].get('extracted_keys', 'N/A')}")
            else:
                print(f"✗ Failed: {results['motionarray'].get('error', 'Unknown error')}")
        
        # Overall summary
        print(f"\n=== SUMMARY ===")
        print(f"Total files downloaded: {results['summary']['total_downloads']}")
        print(f"Overall success: {'✓' if results['summary']['overall_success'] else '✗'}")
        
        # File locations
        print(f"\n=== FILE LOCATIONS ===")
        print(f"Artlist files: {downloader.artlist_dir}")
        print(f"MotionArray files: {downloader.motionarray_dir}")
        
    except Exception as e:
        print(f"✗ Critical error: {e}")
        logger.error(f"Main execution failed: {e}")


if __name__ == "__main__":
    main()