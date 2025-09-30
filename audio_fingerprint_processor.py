#!/usr/bin/env python3
"""
Enhanced Audio Fingerprint Processor with Parallel Processing

This enhanced version adds:
- Parallel processing using ThreadPoolExecutor
- Source-specific processing (Artlist-only, MotionArray-only)
- Process ALL songs from a source (not just batches)
- Better progress tracking and performance monitoring
- Configurable worker threads

Usage:
    # Process ALL Artlist songs with parallel processing
    python enhanced_fingerprint_processor.py --source artlist --workers 8
    
    # Process ALL MotionArray songs with parallel processing  
    python enhanced_fingerprint_processor.py --source motionarray --workers 4
    
    # Process specific asset IDs with parallel processing
    python enhanced_fingerprint_processor.py --asset-ids "12345,67890" --workers 6
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import threading

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
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedAudioFingerprintProcessor:
    """Enhanced processor with parallel processing and source filtering"""
    
    def __init__(self, max_workers: int = 4):
        self.snowflake = SnowflakeConnector()
        self.acoustid = self.setup_chromaprint()
        self.max_workers = max_workers
        self.temp_dirs = {}  # Thread-safe temp directory management
        self.stats_lock = Lock()
        self.stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'start_time': None
        }
        
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
    
    def get_all_assets_by_source(self, source: str) -> List[Dict]:
        """
        Get ALL assets from a specific source (artlist or motionarray)
        """
        if source.lower() == 'artlist':
            query = """
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
                AND af.asset_id IS NULL  -- Only unprocessed
            ORDER BY da.asset_id
            """
        elif source.lower() == 'motionarray':
            query = """
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
                AND af.asset_id IS NULL  -- Only unprocessed
            ORDER BY a.asset_id
            """
        else:
            raise ValueError(f"Invalid source: {source}. Must be 'artlist' or 'motionarray'")
        
        try:
            cursor = self.snowflake.execute_query(query)
            results = cursor.fetchall()
            
            if results:
                columns = [desc[0] for desc in cursor.description]
                assets = [dict(zip(columns, row)) for row in results]
                logger.info(f"📊 Found {len(assets)} unprocessed {source} assets")
                return assets
            else:
                logger.warning(f"⚠️  No unprocessed {source} assets found")
                return []
        except Exception as e:
            logger.error(f"❌ Failed to get {source} assets: {e}")
            return []
    
    def get_asset_file_keys(self, asset_ids: List[str] = None) -> List[Dict]:
        """Get specific asset file keys (original method for compatibility)"""
        if not asset_ids:
            return []
            
        asset_filter = "', '".join(asset_ids)
        
        query = f"""
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
          WHERE da.product_indicator = 1
            AND da.asset_type = 'Music'
            AND sf.role IN ('CORE', 'MP3')
            AND da.asset_id::string IN ('{asset_filter}')
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
          WHERE a.product_indicator = 3
            AND a.asset_sub_type ILIKE '%music%'
            AND b.resolution_format = 1
            AND a.asset_id::string IN ('{asset_filter}')
        )
        SELECT * FROM artlist_base
        UNION ALL
        SELECT * FROM motionarray_base
        """
        
        try:
            cursor = self.snowflake.execute_query(query)
            results = cursor.fetchall()
            
            if results:
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in results]
            else:
                return []
        except Exception as e:
            logger.error(f"❌ Failed to get asset file keys: {e}")
            return []
    
    def get_thread_temp_dir(self) -> Path:
        """Get thread-specific temporary directory"""
        thread_id = threading.current_thread().ident
        if thread_id not in self.temp_dirs:
            self.temp_dirs[thread_id] = tempfile.mkdtemp(prefix=f"audio_fp_thread_{thread_id}_")
        return Path(self.temp_dirs[thread_id])
    
    def cleanup_thread_temp_dir(self):
        """Clean up thread-specific temporary directory"""
        thread_id = threading.current_thread().ident
        if thread_id in self.temp_dirs:
            import shutil
            try:
                shutil.rmtree(self.temp_dirs[thread_id])
                del self.temp_dirs[thread_id]
            except Exception as e:
                logger.warning(f"⚠️  Failed to cleanup temp dir for thread {thread_id}: {e}")
    
    def get_download_url_from_api(self, file_key: str, source: str) -> Optional[str]:
        """Get signed download URL from Artlist/MotionArray API"""
        try:
            import requests
            
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
    
    def download_audio_file(self, file_key: str, source: str, temp_dir: Path) -> Optional[Path]:
        """Download audio file using Artlist/MotionArray API"""
        try:
            download_url = self.get_download_url_from_api(file_key, source)
            if not download_url:
                return None
            
            filename = Path(file_key).name
            if not any(filename.endswith(ext) for ext in ['.mp3', '.wav', '.flac', '.m4a']):
                filename += '.mp3'
            
            temp_file = temp_dir / filename
            
            import requests
            response = requests.get(download_url, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            if temp_file.exists() and temp_file.stat().st_size > 0:
                return temp_file
            else:
                return None
                
        except Exception as e:
            logger.warning(f"❌ Download error: {file_key} - {e}")
            return None
    
    def generate_fingerprint(self, file_path: Path) -> Optional[Tuple[float, str]]:
        """Generate fingerprint with robust error handling"""
        try:
            duration, fingerprint = self.acoustid.fingerprint_file(str(file_path))
            
            if isinstance(fingerprint, bytes):
                fingerprint = fingerprint.decode('utf-8')
            
            if duration > 0 and fingerprint:
                return float(duration), fingerprint
            else:
                return None
                
        except Exception as e:
            logger.warning(f"❌ Fingerprint error: {file_path.name} - {e}")
            return None
    
    def store_fingerprint(self, asset_id: str, file_key: str, format_ext: str, 
                         duration: float, fingerprint: str, file_size: int, source: str):
        """Store fingerprint in Snowflake (thread-safe)"""
        insert_sql = """
        INSERT INTO AI_DATA.AUDIO_FINGERPRINT 
        (ASSET_ID, FILE_KEY, FORMAT, DURATION, FINGERPRINT, FILE_SIZE, SOURCE, PROCESSING_STATUS)
        VALUES (%(asset_id)s, %(file_key)s, %(format)s, %(duration)s, %(fingerprint)s, %(file_size)s, %(source)s, 'SUCCESS')
        """
        
        try:
            # Create a new connection for this thread
            thread_snowflake = SnowflakeConnector()
            thread_snowflake.execute_query(insert_sql, {
                'asset_id': asset_id,
                'file_key': file_key,
                'format': format_ext,
                'duration': duration,
                'fingerprint': fingerprint,
                'file_size': file_size,
                'source': source
            })
            thread_snowflake.close()
        except Exception as e:
            logger.error(f"❌ Failed to store fingerprint: {asset_id} - {e}")
            raise
    
    def store_error(self, asset_id: str, file_key: str, format_ext: str, 
                   file_size: int, source: str, error_message: str):
        """Store processing error in Snowflake (thread-safe)"""
        insert_sql = """
        INSERT INTO AI_DATA.AUDIO_FINGERPRINT 
        (ASSET_ID, FILE_KEY, FORMAT, FILE_SIZE, SOURCE, PROCESSING_STATUS, ERROR_MESSAGE)
        VALUES (%(asset_id)s, %(file_key)s, %(format)s, %(file_size)s, %(source)s, 'ERROR', %(error_message)s)
        """
        
        try:
            thread_snowflake = SnowflakeConnector()
            thread_snowflake.execute_query(insert_sql, {
                'asset_id': asset_id,
                'file_key': file_key,
                'format': format_ext,
                'file_size': file_size,
                'source': source,
                'error_message': error_message
            })
            thread_snowflake.close()
        except Exception as e:
            logger.error(f"❌ Failed to store error: {asset_id} - {e}")
    
    def process_single_asset(self, asset_data: Dict) -> bool:
        """Process a single asset (thread-safe version)"""
        asset_id = asset_data['ASSET_ID']
        file_key = asset_data['FILE_KEY']
        file_format = asset_data.get('FILE_FORMAT', 'mp3')
        file_size = asset_data.get('FILE_SIZE', 0)
        source = asset_data.get('SOURCE', 'artlist')
        
        thread_name = threading.current_thread().name
        logger.info(f"🎵 [{thread_name}] Processing: {asset_id} ({file_key})")
        
        try:
            temp_dir = self.get_thread_temp_dir()
            
            # Download file
            temp_file = self.download_audio_file(file_key, source, temp_dir)
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
            
            # Clean up temp file
            temp_file.unlink()
            
            # Update stats (thread-safe)
            with self.stats_lock:
                self.stats['successful'] += 1
                self.stats['processed'] += 1
            
            logger.info(f"✅ [{thread_name}] Completed: {asset_id} ({duration:.1f}s)")
            return True
            
        except Exception as e:
            logger.error(f"❌ [{thread_name}] Processing failed: {asset_id} - {e}")
            
            self.store_error(asset_id, file_key, file_format, file_size, source, str(e))
            
            # Update stats (thread-safe)
            with self.stats_lock:
                self.stats['failed'] += 1
                self.stats['processed'] += 1
            
            return False
        finally:
            self.cleanup_thread_temp_dir()
    
    def process_assets_parallel(self, assets: List[Dict]) -> Dict:
        """Process assets using parallel processing"""
        if not assets:
            logger.warning("⚠️  No assets to process")
            return {'processed': 0, 'successful': 0, 'failed': 0, 'total_time_minutes': 0}
        
        logger.info(f"🚀 Starting parallel processing of {len(assets)} assets with {self.max_workers} workers")
        
        # Initialize stats
        with self.stats_lock:
            self.stats = {
                'processed': 0,
                'successful': 0,
                'failed': 0,
                'start_time': time.time()
            }
        
        # Process assets in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_asset = {executor.submit(self.process_single_asset, asset): asset for asset in assets}
            
            # Process completed tasks
            for i, future in enumerate(as_completed(future_to_asset), 1):
                try:
                    future.result()  # This will raise any exception that occurred
                except Exception as e:
                    asset = future_to_asset[future]
                    logger.error(f"❌ Task failed for asset {asset.get('ASSET_ID', 'unknown')}: {e}")
                
                # Progress update
                if i % 10 == 0 or i == len(assets):
                    with self.stats_lock:
                        elapsed = time.time() - self.stats['start_time']
                        rate = self.stats['processed'] / elapsed if elapsed > 0 else 0
                        eta = (len(assets) - self.stats['processed']) / rate if rate > 0 else 0
                        
                        logger.info(f"📈 Progress: {self.stats['processed']}/{len(assets)} "
                                  f"({self.stats['processed']/len(assets)*100:.1f}%) | "
                                  f"Rate: {rate:.1f}/sec | ETA: {eta/60:.1f}min | "
                                  f"Success: {self.stats['successful']} | Failed: {self.stats['failed']}")
        
        # Final results
        with self.stats_lock:
            total_time = time.time() - self.stats['start_time']
            results = {
                'processed': self.stats['processed'],
                'successful': self.stats['successful'],
                'failed': self.stats['failed'],
                'total_time_minutes': total_time / 60,
                'rate_per_second': self.stats['processed'] / total_time if total_time > 0 else 0
            }
        
        logger.info(f"✅ Parallel processing complete: {results['successful']}/{results['processed']} successful "
                   f"({total_time/60:.1f}min, {results['rate_per_second']:.1f} assets/sec)")
        
        return results

def main():
    parser = argparse.ArgumentParser(description='Enhanced Audio Fingerprint Processor with Parallel Processing')
    parser.add_argument('--source', choices=['artlist', 'motionarray'], 
                       help='Process ALL songs from specific source')
    parser.add_argument('--asset-ids', help='Comma-separated list of asset IDs to process')
    parser.add_argument('--workers', type=int, default=4, 
                       help='Number of parallel workers (default: 4)')
    parser.add_argument('--stats', action='store_true', help='Show processing statistics')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.source and not args.asset_ids and not args.stats:
        parser.error("Must specify either --source, --asset-ids, or --stats")
    
    # Initialize processor
    try:
        processor = EnhancedAudioFingerprintProcessor(max_workers=args.workers)
        processor.ensure_table_exists()
    except Exception as e:
        logger.error(f"❌ Failed to initialize processor: {e}")
        return 1
    
    # Show stats if requested
    if args.stats:
        # Use original processor for stats (compatibility)
        from audio_fingerprint_processor import AudioFingerprintProcessor
        original_processor = AudioFingerprintProcessor()
        stats = original_processor.get_processing_stats()
        print("\n📊 Processing Statistics:")
        for key, value in stats.items():
            print(f"   {key}: {value}")
        return 0
    
    # Get assets to process
    try:
        if args.source:
            logger.info(f"🎯 Processing ALL {args.source} songs")
            assets = processor.get_all_assets_by_source(args.source)
        elif args.asset_ids:
            asset_ids = [aid.strip() for aid in args.asset_ids.split(',')]
            logger.info(f"🎯 Processing specific assets: {asset_ids}")
            assets = processor.get_asset_file_keys(asset_ids)
        else:
            assets = []
        
        if not assets:
            logger.warning("⚠️  No assets found to process")
            return 0
        
        # Process assets
        results = processor.process_assets_parallel(assets)
        
        # Print final results
        print(f"\n📊 Final Results:")
        print(f"   Total Assets: {len(assets)}")
        print(f"   Processed: {results['processed']}")
        print(f"   Successful: {results['successful']}")
        print(f"   Failed: {results['failed']}")
        print(f"   Success Rate: {results['successful']/results['processed']*100:.1f}%")
        print(f"   Total Time: {results['total_time_minutes']:.1f} minutes")
        print(f"   Processing Rate: {results['rate_per_second']:.1f} assets/second")
        print(f"   Parallel Workers: {args.workers}")
        
        return 0 if results['failed'] == 0 else 1
        
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
