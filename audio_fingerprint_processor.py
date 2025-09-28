#!/usr/bin/env python3
"""
Audio Fingerprint Processor for Production

This script processes audio files, generates fingerprints, and stores them in Snowflake.
Designed for production use with the BI PROD AI DATA warehouse.

Features:
- Downloads audio files from S3/URLs using file keys
- Generates Chromaprint fingerprints
- Stores results in Snowflake AUDIO_FINGERPRINT table
- Handles batch processing and error recovery
- Supports resume functionality for interrupted jobs

Usage:
    python audio_fingerprint_processor.py --asset-ids 12345,67890
    python audio_fingerprint_processor.py --batch-size 100 --resume
"""

import os
import sys
import subprocess
import argparse
import json
import tempfile
from pathlib import Path
import time
from typing import List, Dict, Optional, Tuple
import logging

# Auto-restart with correct environment if needed
if 'DYLD_LIBRARY_PATH' not in os.environ or '/opt/homebrew/lib' not in os.environ.get('DYLD_LIBRARY_PATH', ''):
    env = os.environ.copy()
    env['DYLD_LIBRARY_PATH'] = '/opt/homebrew/lib:' + env.get('DYLD_LIBRARY_PATH', '')
    result = subprocess.run([sys.executable] + sys.argv, env=env)
    sys.exit(result.returncode)

import ctypes
import pandas as pd
from snowflake_utils import SnowflakeConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('audio_fingerprint_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AudioFingerprintProcessor:
    """Processes audio files and stores fingerprints in Snowflake"""
    
    def __init__(self):
        self.snowflake = SnowflakeConnector()  # Uses existing credential management
        self.acoustid = self.setup_chromaprint()
        self.temp_dir = None
        
    def setup_chromaprint(self):
        """Set up Chromaprint library and environment"""
        try:
            chromaprint_lib = ctypes.CDLL('/opt/homebrew/lib/libchromaprint.dylib')
            import acoustid
            
            fpcalc_path = "/opt/homebrew/bin/fpcalc"
            os.environ['FPCALC_COMMAND'] = fpcalc_path
            acoustid.FPCALC_COMMAND = fpcalc_path
            
            logger.info("✅ Chromaprint setup successful")
            return acoustid
        except Exception as e:
            logger.error(f"❌ Chromaprint setup failed: {e}")
            raise
    
    def ensure_table_exists(self):
        """Create AUDIO_FINGERPRINT table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS AI_DATA.AUDIO_FINGERPRINT (
            ASSET_ID VARCHAR(50) NOT NULL,
            FILE_KEY VARCHAR(500) NOT NULL,
            FORMAT VARCHAR(10),
            DURATION FLOAT,
            FINGERPRINT TEXT,
            FILE_SIZE BIGINT,
            SOURCE VARCHAR(20),
            PROCESSING_STATUS VARCHAR(20) DEFAULT 'SUCCESS',
            ERROR_MESSAGE TEXT,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (ASSET_ID, FILE_KEY)
        )
        """
        
        try:
            self.snowflake.execute_query(create_table_sql)
            logger.info("✅ AUDIO_FINGERPRINT table ready")
        except Exception as e:
            logger.error(f"❌ Failed to create table: {e}")
            raise
    
    def get_asset_file_keys(self, asset_ids: List[str] = None, batch_size: int = 100) -> List[Dict]:
        """
        Get asset file keys from Artlist and MotionArray tables
        """
        if asset_ids:
            # Get specific assets - combine both Artlist and MotionArray
            asset_filter = "', '".join(asset_ids)
            
            artlist_query = f"""
            WITH base AS (
              SELECT
                da.asset_id::string as asset_id,
                sf.filekey AS file_key,
                CASE WHEN sf.role = 'CORE' THEN 'wav' ELSE LOWER(sf.role) END AS file_format,
                0 as file_size,
                'artlist' as source
              FROM BI_PROD.dwh.DIM_ASSETS da
              JOIN ODS_PROD.cross_products_ods.POSTGRES_ASM_ASSET a
                ON da.asset_id::string = a.externalid::int::string
              JOIN ODS_PROD.cross_products_ods.POSTGRES_ASM_songFILE sf
                ON a.id = sf.songid
              WHERE da.product_indicator = 1
                AND da.asset_type = 'Music'
                AND sf.role IN ('CORE', 'MP3')
                AND da.asset_id::string IN ('{asset_filter}')
            )
            SELECT * FROM base
            
            UNION ALL
            
            SELECT
              a.asset_id::string as asset_id,
              b.guid AS file_key,
              CASE
                WHEN pf.format_id = 1 THEN 'wav'
                WHEN pf.format_id = 2 THEN 'mp3'
                WHEN pf.format_id = 3 THEN 'aiff'
                ELSE 'mp3'
              END AS file_format,
              0 as file_size,
              'motionarray' as source
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
              AND a.asset_id::string IN ('{asset_filter}')
            """
        else:
            # Get batch of unprocessed assets from both sources
            artlist_query = f"""
            WITH artlist_base AS (
              SELECT
                da.asset_id::string as asset_id,
                sf.filekey AS file_key,
                CASE WHEN sf.role = 'CORE' THEN 'wav' ELSE LOWER(sf.role) END AS file_format,
                0 as file_size,
                'artlist' as source
              FROM BI_PROD.dwh.DIM_ASSETS da
              JOIN ODS_PROD.cross_products_ods.POSTGRES_ASM_ASSET a
                ON da.asset_id::string = a.externalid::int::string
              JOIN ODS_PROD.cross_products_ods.POSTGRES_ASM_songFILE sf
                ON a.id = sf.songid
              LEFT JOIN AI_DATA.AUDIO_FINGERPRINT af 
                ON da.asset_id::string = af.asset_id AND sf.filekey = af.file_key
              WHERE da.product_indicator = 1
                AND da.asset_type = 'Music'
                AND sf.role IN ('CORE', 'MP3')
                AND af.asset_id IS NULL
              LIMIT {min(50, batch_size // 2)}
            ),
            motionarray_base AS (
              SELECT
                a.asset_id::string as asset_id,
                b.guid AS file_key,
                CASE
                  WHEN pf.format_id = 1 THEN 'wav'
                  WHEN pf.format_id = 2 THEN 'mp3'
                  WHEN pf.format_id = 3 THEN 'aiff'
                  ELSE 'mp3'
                END AS file_format,
                0 as file_size,
                'motionarray' as source
              FROM BI_PROD.dwh.DIM_ASSETS a
              JOIN ODS_PROD.motion_array_ods.MYSQL_PRODUCT_CMS_RESOLUTIONS b
                ON a.asset_id::string = b.product_id::string
              JOIN ODS_PROD.motion_array_ods.MYSQL_PRODUCT_CMS_AUDIO_RESOLUTIONS c
                ON b.id = c.parent_id
              LEFT JOIN ODS_PROD.motion_array_ods.MYSQL_PRODUCT_FORMAT pf
                ON pf.product_id = a.asset_id
              LEFT JOIN AI_DATA.AUDIO_FINGERPRINT af 
                ON a.asset_id::string = af.asset_id AND b.guid = af.file_key
              WHERE a.product_indicator = 3
                AND a.asset_sub_type ILIKE '%music%'
                AND b.resolution_format = 1
                AND af.asset_id IS NULL
              LIMIT {min(50, batch_size // 2)}
            )
            SELECT * FROM artlist_base
            UNION ALL
            SELECT * FROM motionarray_base
            """
        
        try:
            cursor = self.snowflake.execute_query(artlist_query)
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            if results:
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in results]
            else:
                return []
        except Exception as e:
            logger.error(f"❌ Failed to get asset file keys: {e}")
            return []
    
    def get_download_url_from_api(self, file_key: str, source: str) -> Optional[str]:
        """Get signed download URL from Artlist/MotionArray API"""
        try:
            import requests
            
            # Use the same API as bulk_downloader
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
            
            # Extract download URL from API response
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
    
    def download_audio_file(self, file_key: str, source: str, temp_dir: Path) -> Optional[Path]:
        """
        Download audio file using Artlist/MotionArray API
        """
        try:
            # Get signed download URL from API
            download_url = self.get_download_url_from_api(file_key, source)
            if not download_url:
                return None
            
            # Extract filename from file_key
            filename = Path(file_key).name
            if not any(filename.endswith(ext) for ext in ['.mp3', '.wav', '.flac', '.m4a']):
                filename += '.mp3'  # Default extension
            
            temp_file = temp_dir / filename
            
            # Download using requests (same as utils.py)
            import requests
            response = requests.get(download_url, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            if temp_file.exists() and temp_file.stat().st_size > 0:
                file_size = temp_file.stat().st_size
                logger.info(f"✅ Downloaded: {filename} ({file_size} bytes)")
                
                # Check if it's actually an audio file by looking at first few bytes
                with open(temp_file, 'rb') as f:
                    header = f.read(16)
                    header_hex = header.hex()[:32]
                    logger.debug(f"📄 File header: {header_hex}")
                    
                    # Check for common audio file signatures
                    if header.startswith(b'ID3') or header[4:8] == b'ftyp' or header.startswith(b'RIFF'):
                        logger.debug(f"✅ Appears to be valid audio file")
                    else:
                        logger.warning(f"⚠️  File may not be valid audio - header: {header_hex}")
                
                return temp_file
            else:
                logger.warning(f"⚠️  Download failed or empty file: {filename}")
                return None
                
        except Exception as e:
            logger.warning(f"❌ Download error: {file_key} - {e}")
            return None
    
    def generate_fingerprint(self, file_path: Path) -> Optional[Tuple[float, str]]:
        """Generate fingerprint with robust error handling"""
        try:
            # Check file size (skip very large files)
            file_size = file_path.stat().st_size
            logger.debug(f"📁 File size: {file_size / (1024*1024):.1f}MB")

            # Generate fingerprint
            duration, fingerprint = self.acoustid.fingerprint_file(str(file_path))
            
            # Ensure fingerprint is a string (not binary)
            if isinstance(fingerprint, bytes):
                fingerprint = fingerprint.decode('utf-8')
            
            logger.info(f"🎵 Generated fingerprint: duration={duration:.1f}s, fp_type={type(fingerprint)}, fp_len={len(fingerprint) if fingerprint else 0}")
            
            if duration > 0 and fingerprint:
                logger.debug(f"✅ Fingerprint generated: {file_path.name} ({duration:.1f}s)")
                return float(duration), fingerprint
            else:
                logger.warning(f"⚠️  Invalid fingerprint data: {file_path.name}")
                return None
                
        except Exception as e:
            logger.warning(f"❌ Fingerprint error: {file_path.name} - {e}")
            return None
    
    def store_fingerprint(self, asset_id: str, file_key: str, format_ext: str, 
                         duration: float, fingerprint: str, file_size: int, source: str):
        """Store fingerprint in Snowflake"""
        insert_sql = """
        INSERT INTO AI_DATA.AUDIO_FINGERPRINT 
        (ASSET_ID, FILE_KEY, FORMAT, DURATION, FINGERPRINT, FILE_SIZE, SOURCE, PROCESSING_STATUS)
        VALUES (%(asset_id)s, %(file_key)s, %(format)s, %(duration)s, %(fingerprint)s, %(file_size)s, %(source)s, 'SUCCESS')
        """
        
        try:
            self.snowflake.execute_query(insert_sql, {
                'asset_id': asset_id,
                'file_key': file_key,
                'format': format_ext,
                'duration': duration,
                'fingerprint': fingerprint,
                'file_size': file_size,
                'source': source
            })
            logger.debug(f"✅ Stored fingerprint: {asset_id}")
        except Exception as e:
            logger.error(f"❌ Failed to store fingerprint: {asset_id} - {e}")
            raise
    
    def store_error(self, asset_id: str, file_key: str, format_ext: str, 
                   file_size: int, source: str, error_message: str):
        """Store processing error in Snowflake"""
        insert_sql = """
        INSERT INTO AI_DATA.AUDIO_FINGERPRINT 
        (ASSET_ID, FILE_KEY, FORMAT, FILE_SIZE, SOURCE, PROCESSING_STATUS, ERROR_MESSAGE)
        VALUES (%(asset_id)s, %(file_key)s, %(format)s, %(file_size)s, %(source)s, 'ERROR', %(error_message)s)
        """
        
        try:
            self.snowflake.execute_query(insert_sql, {
                'asset_id': asset_id,
                'file_key': file_key,
                'format': format_ext,
                'file_size': file_size,
                'source': source,
                'error_message': error_message
            })
        except Exception as e:
            logger.error(f"❌ Failed to store error: {asset_id} - {e}")
    
    def process_asset(self, asset_data: Dict) -> bool:
        """Process a single asset"""
        # Debug: print the asset_data structure
        logger.debug(f"Asset data structure: {asset_data}")
        logger.debug(f"Asset data keys: {list(asset_data.keys()) if isinstance(asset_data, dict) else 'Not a dict'}")
        
        asset_id = asset_data['ASSET_ID']
        file_key = asset_data['FILE_KEY']
        file_format = asset_data.get('FILE_FORMAT', 'mp3')
        file_size = asset_data.get('FILE_SIZE', 0)
        source = asset_data.get('SOURCE', 'artlist')
        
        logger.info(f"🎵 Processing asset: {asset_id} ({file_key})")
        
        try:
            # Download file using API
            temp_file = self.download_audio_file(file_key, source, self.temp_dir)
            if not temp_file:
                self.store_error(asset_id, file_key, file_format, file_size, source, "Download failed")
                return False
            
            # Generate fingerprint
            result = self.generate_fingerprint(temp_file)
            if not result:
                self.store_error(asset_id, file_key, file_format, file_size, source, "Fingerprint generation failed")
                return False
            
            duration, fingerprint = result
            
            # Store in Snowflake
            self.store_fingerprint(asset_id, file_key, file_format, duration, fingerprint, file_size, source)
            
            # Clean up temp file to save disk space
            temp_file.unlink()
            logger.debug(f"🗑️  Cleaned up temp file: {temp_file.name}")
            
            logger.info(f"✅ Completed: {asset_id} ({duration:.1f}s)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Processing failed: {asset_id} - {e}")
            
            # Clean up temp file even on error
            if 'temp_file' in locals() and temp_file and temp_file.exists():
                temp_file.unlink()
                logger.debug(f"🗑️  Cleaned up temp file after error: {temp_file.name}")
            
            self.store_error(asset_id, file_key, file_format, file_size, source, str(e))
            return False
    
    def process_batch(self, asset_ids: List[str] = None, batch_size: int = 100) -> Dict:
        """Process a batch of assets"""
        logger.info(f"🚀 Starting batch processing (batch_size: {batch_size})")
        
        # Ensure table exists
        self.ensure_table_exists()
        
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            self.temp_dir = Path(temp_dir)
            
            # Get assets to process
            assets = self.get_asset_file_keys(asset_ids, batch_size)
            if not assets:
                logger.warning("⚠️  No assets found to process")
                return {'processed': 0, 'successful': 0, 'failed': 0}
            
            logger.info(f"📊 Found {len(assets)} assets to process")
            
            # Process assets
            successful = 0
            failed = 0
            start_time = time.time()
            
            for i, asset_data in enumerate(assets, 1):
                logger.info(f"Progress: {i}/{len(assets)} ({i/len(assets)*100:.1f}%)")
                
                if self.process_asset(asset_data):
                    successful += 1
                else:
                    failed += 1
                
                # Progress update every 10 assets
                if i % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = i / elapsed
                    eta = (len(assets) - i) / rate if rate > 0 else 0
                    logger.info(f"📈 Rate: {rate:.1f} assets/sec, ETA: {eta/60:.1f}min")
            
            total_time = time.time() - start_time
            
            results = {
                'processed': len(assets),
                'successful': successful,
                'failed': failed,
                'total_time_minutes': total_time / 60,
                'rate_per_second': len(assets) / total_time
            }
            
            logger.info(f"✅ Batch complete: {successful}/{len(assets)} successful ({total_time/60:.1f}min)")
            return results
    
    def get_processing_stats(self) -> Dict:
        """Get processing statistics from Snowflake"""
        stats_sql = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN processing_status = 'SUCCESS' THEN 1 END) as successful,
            COUNT(CASE WHEN processing_status = 'ERROR' THEN 1 END) as failed,
            AVG(CASE WHEN processing_status = 'SUCCESS' THEN duration END) as avg_duration,
            MIN(created_at) as first_processed,
            MAX(created_at) as last_processed
        FROM AI_DATA.AUDIO_FINGERPRINT
        """
        
        try:
            cursor = self.snowflake.execute_query(stats_sql)
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
            else:
                return {}
        except Exception as e:
            logger.error(f"❌ Failed to get stats: {e}")
            return {}

def main():
    parser = argparse.ArgumentParser(description='Audio Fingerprint Processor for Production')
    parser.add_argument('--asset-ids', help='Comma-separated list of asset IDs to process')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    parser.add_argument('--resume', action='store_true', help='Resume processing unprocessed assets')
    parser.add_argument('--stats', action='store_true', help='Show processing statistics')
    
    args = parser.parse_args()
    
    # Initialize processor (uses existing Snowflake credential management)
    try:
        processor = AudioFingerprintProcessor()
    except Exception as e:
        logger.error(f"❌ Failed to initialize processor: {e}")
        return 1
    
    # Show stats if requested
    if args.stats:
        stats = processor.get_processing_stats()
        print("\n📊 Processing Statistics:")
        for key, value in stats.items():
            print(f"   {key}: {value}")
        return 0
    
    # Parse asset IDs
    asset_ids = None
    if args.asset_ids:
        asset_ids = [aid.strip() for aid in args.asset_ids.split(',')]
        logger.info(f"🎯 Processing specific assets: {asset_ids}")
    elif args.resume:
        logger.info("🔄 Resuming processing of unprocessed assets")
    else:
        logger.info(f"🚀 Processing next {args.batch_size} unprocessed assets")
    
    # Process batch
    try:
        results = processor.process_batch(asset_ids, args.batch_size)
        
        print(f"\n📊 Final Results:")
        print(f"   Processed: {results['processed']} assets")
        print(f"   Successful: {results['successful']}")
        print(f"   Failed: {results['failed']}")
        print(f"   Success Rate: {results['successful']/results['processed']*100:.1f}%")
        print(f"   Total Time: {results['total_time_minutes']:.1f} minutes")
        print(f"   Processing Rate: {results['rate_per_second']:.1f} assets/second")
        
        return 0 if results['failed'] == 0 else 1
        
    except Exception as e:
        logger.error(f"❌ Batch processing failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
