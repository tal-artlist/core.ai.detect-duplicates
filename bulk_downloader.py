#!/usr/bin/env python3
"""
Bulk downloader for Artlist content artifacts.

This script provides functions to:
1. Obtain keys from SQL queries
2. Bulk download artifacts using the Artlist API
3. Handle download responses and file management
"""

import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional
import requests
from urllib.parse import urlparse

try:
    import snowflake.connector
    from google.cloud import secretmanager
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    logging.warning("Snowflake/GCP dependencies not available. Install with: pip install snowflake-connector-python google-cloud-secret-manager")


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
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
        self.artlist_api_url = "https://oapi-int.artlist.io/v1/content/bulkDownloadArtifacts"
        self.artlist_headers = {
            'service-host': 'core.content.cms.api',
            'Content-Type': 'application/json'
        }
        
        # MotionArray uses the same API endpoint and headers
        self.motionarray_api_url = "https://oapi-int.artlist.io/v1/content/bulkDownloadArtifacts"
        self.motionarray_headers = {
            'service-host': 'core.content.cms.api',
            'Content-Type': 'application/json'
        }
        
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for each platform
        self.artlist_dir = self.download_dir / "artlist"
        self.motionarray_dir = self.download_dir / "motionarray"
        self.artlist_dir.mkdir(exist_ok=True)
        self.motionarray_dir.mkdir(exist_ok=True)
    
    def get_snowflake_secret(self, secret_id="ai_team_snowflake_credentials", project_id="889375371783"):
        """
        Get Snowflake credentials from Google Cloud Secret Manager.
        
        Args:
            secret_id: Secret ID in Google Cloud
            project_id: Google Cloud project ID
            
        Returns:
            Dictionary with Snowflake credentials
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError("Snowflake/GCP dependencies not available")
            
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return json.loads(response.payload.data.decode("UTF-8"))
        except Exception as e:
            logger.warning(f"Failed to get credentials from Google Cloud: {e}")
            return None
    
    def get_legacy_secret(self, secret_name="snowflake-ai_team-artlist", env="prd"):
        """
        Fallback method for legacy secret retrieval.
        
        Args:
            secret_name: Name of the secret
            env: Environment (prd, dev, etc.)
            
        Returns:
            Dictionary with credentials or None
        """
        try:
            # This would need to be implemented based on your legacy system
            # For now, return None to indicate fallback failed
            logger.warning("Legacy secret retrieval not implemented")
            return None
        except Exception as e:
            logger.warning(f"Failed to get legacy credentials: {e}")
            return None
    
    def open_snowflake_cursor(self):
        """
        Open Snowflake cursor with proper authentication.
        
        Returns:
            Snowflake cursor object
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError("Snowflake dependencies not available")
        
        # Try Google Cloud first
        creds = self.get_snowflake_secret()
        
        # Fallback to legacy method
        if creds is None:
            creds = self.get_legacy_secret()
        
        if creds is None:
            raise Exception("Could not obtain Snowflake credentials from any source")
        
        # Add defaults
        if "database" not in creds:
            creds["database"] = "BI_PROD"
        if "schema" not in creds:
            creds["schema"] = "AI_DATA"
        
        # Connect
        conn = snowflake.connector.connect(**creds)
        return conn.cursor()
    
    def get_artlist_keys_from_snowflake(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get Artlist file keys from Snowflake using the provided query.
        Only gets WAV and MP3 formats, one file per format per asset.
        
        Args:
            limit: Maximum number of assets to retrieve
            
        Returns:
            List of dictionaries with asset_id and key_format_pairs
        """
        query = f"""
        WITH base AS (
          SELECT
            da.asset_id,
            sf.filekey                    AS file_key,
            sf.role                       AS format,
            sf.createdat                  AS created_at
          FROM BI_PROD.dwh.DIM_ASSETS da
          JOIN ODS_PROD.cross_products_ods.POSTGRES_ASM_ASSET a
            ON da.asset_id::string = a.externalid::int::string
          JOIN ODS_PROD.cross_products_ods.POSTGRES_ASM_songFILE sf
            ON a.id = sf.songid
          WHERE da.product_indicator = 1
            AND da.asset_type = 'Music'
            AND sf.role IN ('CORE', 'MP3')  -- Only WAV (CORE) and MP3 formats
        ),
        one_per_format AS (
          SELECT asset_id, format, file_key, created_at
          FROM (
            SELECT
              asset_id,
              format,
              file_key,
              created_at,
              ROW_NUMBER() OVER (
                PARTITION BY asset_id, format
                ORDER BY created_at DESC, file_key
              ) AS rn
            FROM base
          )
          WHERE rn = 1
        )
        SELECT
          asset_id,
          COUNT(*) AS num_file_keys,
          ARRAY_AGG(file_key) AS file_keys,
          ARRAY_AGG(format)   AS formats,
          ARRAY_AGG(OBJECT_CONSTRUCT('file_key', file_key, 'format', format)) AS key_format_pairs,
          OBJECT_AGG(format, TO_VARIANT(file_key)) AS format_to_file_key
        FROM one_per_format
        GROUP BY asset_id
        ORDER BY num_file_keys DESC, asset_id
        LIMIT {limit};
        """
        
        try:
            cursor = self.open_snowflake_cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Convert results to list of dictionaries
            columns = [desc[0] for desc in cursor.description]
            data = []
            
            for row in results:
                row_dict = dict(zip(columns, row))
                data.append(row_dict)
            
            logger.info(f"Retrieved {len(data)} Artlist assets from Snowflake")
            return data
            
        except Exception as e:
            logger.error(f"Failed to get Artlist keys from Snowflake: {e}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()
    
    def get_motionarray_keys_from_snowflake(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get MotionArray file keys from Snowflake using the provided query.
        Gets one file per format per asset.
        
        Args:
            limit: Maximum number of assets to retrieve
            
        Returns:
            List of dictionaries with asset_id and key_format_pairs
        """
        query = f"""
        WITH base AS (
          SELECT
            a.asset_id,
            b.guid AS file_key,
            CASE
              WHEN pf.format_id = 1 THEN 'WAV'
              WHEN pf.format_id = 2 THEN 'MP3'
              WHEN pf.format_id = 3 THEN 'AIFF'
              ELSE NULL
            END AS format,
            c.created_at
          FROM BI_PROD.dwh.DIM_ASSETS a
          JOIN ODS_PROD.motion_array_ods.MYSQL_PRODUCT_CMS_RESOLUTIONS b
            ON a.asset_id::string = b.product_id::string
          JOIN ODS_PROD.motion_array_ods.MYSQL_PRODUCT_CMS_AUDIO_RESOLUTIONS c
            ON b.id = c.parent_id
          LEFT JOIN ODS_PROD.motion_array_ods.MYSQL_PRODUCT_FORMAT pf
            ON pf.product_id = a.asset_id
          WHERE a.product_indicator = 3
            AND a.asset_sub_type ILIKE '%music%'
            AND b.resolution_format = 1
            AND format IS NOT NULL
        ),
        one_per_format AS (
          SELECT asset_id, format, file_key, created_at
          FROM (
            SELECT
              asset_id,
              format,
              file_key,
              created_at,
              ROW_NUMBER() OVER (
                PARTITION BY asset_id, format
                ORDER BY created_at DESC, file_key
              ) AS rn
            FROM base
          )
          WHERE rn = 1
        )
        SELECT
          asset_id,
          COUNT(*) AS num_file_keys,
          ARRAY_AGG(file_key) AS file_keys,
          ARRAY_AGG(format)   AS formats,
          ARRAY_AGG(OBJECT_CONSTRUCT('file_key', file_key, 'format', format)) AS key_format_pairs,
          OBJECT_AGG(format, TO_VARIANT(file_key)) AS format_to_file_key
        FROM one_per_format
        GROUP BY asset_id
        ORDER BY num_file_keys DESC, asset_id
        LIMIT {limit};
        """
        
        try:
            cursor = self.open_snowflake_cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Convert results to list of dictionaries
            columns = [desc[0] for desc in cursor.description]
            data = []
            
            for row in results:
                row_dict = dict(zip(columns, row))
                data.append(row_dict)
            
            logger.info(f"Retrieved {len(data)} MotionArray assets from Snowflake")
            return data
            
        except Exception as e:
            logger.error(f"Failed to get MotionArray keys from Snowflake: {e}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()
        
    def get_keys_from_sql(self, db_path: str, query: str) -> List[str]:
        """
        Execute SQL query to obtain download keys.
        
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
        if platform.lower() == "artlist":
            api_url = self.artlist_api_url
            headers = self.artlist_headers
        elif platform.lower() == "motionarray":
            api_url = self.motionarray_api_url
            headers = self.motionarray_headers
        else:
            raise ValueError(f"Unsupported platform: {platform}")
        
        payload = {"keys": keys}
        
        try:
            logger.info(f"Making {platform} bulk download request for {len(keys)} keys")
            response = requests.post(
                api_url,
                headers=headers,
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
        if platform.lower() == "artlist":
            target_dir = self.artlist_dir
        elif platform.lower() == "motionarray":
            target_dir = self.motionarray_dir
        else:
            target_dir = self.download_dir
        
        for key, url in download_urls.items():
            try:
                logger.info(f"Downloading {platform} file for key: {key}")
                
                # Get filename from URL or use key as fallback
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path) or f"{key}.bin"
                file_path = target_dir / filename
                
                # Download file
                response = requests.get(url, stream=True, timeout=60)
                response.raise_for_status()
                
                # Save file
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                logger.info(f"Successfully downloaded: {file_path}")
                results[key] = True
                
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
            
            # Extract download URLs from actual API response structure
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
    
    def extract_keys_from_snowflake_data(self, data: List[Dict[str, Any]]) -> List[str]:
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
        unique_keys = []
        seen = set()
        for key in keys:
            if key not in seen:
                unique_keys.append(key)
                seen.add(key)
        
        if len(unique_keys) != len(keys):
            logger.info(f"Removed {len(keys) - len(unique_keys)} duplicate keys")
        
        return unique_keys
    
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
            snowflake_data = self.get_artlist_keys_from_snowflake(limit)
            
            if not snowflake_data:
                return {
                    'success': False,
                    'platform': 'artlist',
                    'error': 'No data retrieved from Snowflake'
                }
            
            # Extract keys
            keys = self.extract_keys_from_snowflake_data(snowflake_data)
            
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
            snowflake_data = self.get_motionarray_keys_from_snowflake(limit)
            
            if not snowflake_data:
                return {
                    'success': False,
                    'platform': 'motionarray',
                    'error': 'No data retrieved from Snowflake'
                }
            
            # Extract keys
            keys = self.extract_keys_from_snowflake_data(snowflake_data)
            
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
        artlist_success = results['artlist']['success'] if results['artlist'] else False
        motionarray_success = results['motionarray']['success'] if results['motionarray'] else False
        
        artlist_downloads = results['artlist'].get('downloads_successful', 0) if results['artlist'] else 0
        motionarray_downloads = results['motionarray'].get('downloads_successful', 0) if results['motionarray'] else 0
        
        results['summary'] = {
            'overall_success': artlist_success or motionarray_success,
            'artlist_success': artlist_success,
            'motionarray_success': motionarray_success,
            'total_downloads': artlist_downloads + motionarray_downloads,
            'artlist_downloads': artlist_downloads,
            'motionarray_downloads': motionarray_downloads
        }
        
        logger.info(f"Download summary: Total={results['summary']['total_downloads']}, "
                   f"Artlist={artlist_downloads}, MotionArray={motionarray_downloads}")
        
        return results


def main():
    """Example usage of the bulk downloader."""
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


def test_with_example_keys():
    """Test function using the original example keys."""
    downloader = BulkDownloader()
    
    print("\n=== TESTING WITH EXAMPLE KEYS ===")
    
    # Example keys from original curl command
    example_keys = [
        "artlist-dev-sfx-17606-objectFile-283038",
        "artlist-dev-sfx-17606-objectFile-270532"
    ]
    
    # Test Artlist download
    result = downloader.process_bulk_download(example_keys, "artlist")
    
    print("Test Results:")
    print(json.dumps(result, indent=2))
    
    if result['success']:
        print(f"\n✓ Test completed successfully!")
        print(f"Downloaded {result['downloads_successful']} out of {result['downloads_attempted']} files")
    else:
        print(f"\n✗ Test failed: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_with_example_keys()
    else:
        main()
