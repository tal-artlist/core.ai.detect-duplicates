#!/usr/bin/env python3


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
from threading import local
import platform
import shutil

# Platform-specific library path detection
SYSTEM = platform.system()

# Auto-restart with correct environment if needed (platform-specific)
if SYSTEM == 'Darwin':  # macOS
    if 'DYLD_LIBRARY_PATH' not in os.environ or '/opt/homebrew/lib' not in os.environ.get('DYLD_LIBRARY_PATH', ''):
        env = os.environ.copy()
        env['DYLD_LIBRARY_PATH'] = '/opt/homebrew/lib:' + env.get('DYLD_LIBRARY_PATH', '')
        result = subprocess.run([sys.executable] + sys.argv, env=env)
        sys.exit(result.returncode)
elif SYSTEM == 'Linux':
    # On Linux, LD_LIBRARY_PATH may be needed for custom installs
    # Usually system packages work without this
    pass

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

class AudioFingerprintProcessor:
    """Processor with parallel processing and source filtering"""
    
    def __init__(self, max_workers: int = 4):
        self.snowflake = SnowflakeConnector()
        self.acoustid = self.setup_chromaprint()
        self.max_workers = max_workers
        self.temp_dirs = {}  # Thread-safe temp directory management
        self.thread_local = local()  # Thread-local storage for connections
        self.stats_lock = Lock()
        self.stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'start_time': None
        }
        
    def setup_chromaprint(self):
        """Set up Chromaprint library and environment (cross-platform)"""
        try:
            # Platform-specific library loading
            chromaprint_lib = None
            if SYSTEM == 'Darwin':  # macOS
                lib_paths = [
                    '/opt/homebrew/lib/libchromaprint.dylib',
                    '/usr/local/lib/libchromaprint.dylib'
                ]
                for lib_path in lib_paths:
                    if os.path.exists(lib_path):
                        chromaprint_lib = ctypes.CDLL(lib_path)
                        logger.info(f"✅ Loaded chromaprint library from: {lib_path}")
                        break
            elif SYSTEM == 'Linux':
                lib_paths = [
                    'libchromaprint.so.1',  # Try system library first
                    'libchromaprint.so',
                    '/lib/x86_64-linux-gnu/libchromaprint.so.1',
                    '/lib/x86_64-linux-gnu/libchromaprint.so',
                    '/usr/lib/x86_64-linux-gnu/libchromaprint.so.1',
                    '/usr/lib/libchromaprint.so.1',
                    '/usr/local/lib/libchromaprint.so.1'
                ]
                for lib_path in lib_paths:
                    try:
                        chromaprint_lib = ctypes.CDLL(lib_path)
                        logger.info(f"✅ Loaded chromaprint library: {lib_path}")
                        break
                    except OSError:
                        continue
            
            if not chromaprint_lib:
                raise RuntimeError(f"Could not find chromaprint library for {SYSTEM}")
            
            # Import acoustid after library is loaded
            import acoustid
            
            # Find fpcalc binary
            fpcalc_path = shutil.which('fpcalc')
            if not fpcalc_path:
                # Try platform-specific paths
                if SYSTEM == 'Darwin':
                    candidate_paths = ['/opt/homebrew/bin/fpcalc', '/usr/local/bin/fpcalc']
                else:  # Linux
                    candidate_paths = ['/usr/bin/fpcalc', '/usr/local/bin/fpcalc']
                
                for path in candidate_paths:
                    if os.path.exists(path):
                        fpcalc_path = path
                        break
            
            if not fpcalc_path:
                if SYSTEM == 'Linux':
                    raise RuntimeError(
                        f"Could not find fpcalc binary. Please install it:\n"
                        f"  Ubuntu/Debian: sudo apt-get install libchromaprint-tools\n"
                        f"  Fedora/RHEL: sudo dnf install chromaprint-tools\n"
                        f"  Arch: sudo pacman -S chromaprint"
                    )
                else:
                    raise RuntimeError(
                        f"Could not find fpcalc binary. Please install it:\n"
                        f"  macOS: brew install chromaprint"
                    )
            
            os.environ['FPCALC_COMMAND'] = fpcalc_path
            acoustid.FPCALC_COMMAND = fpcalc_path
            
            logger.info(f"✅ Chromaprint setup successful on {SYSTEM} (fpcalc: {fpcalc_path})")
            return acoustid
        except Exception as e:
            logger.error(f"❌ Chromaprint setup failed on {SYSTEM}: {e}")
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
    
    def get_all_assets_by_source(self, source: str, retry_errors: bool = False) -> List[Dict]:
        """
        Get ALL assets from a specific source (artlist or motionarray)
        
        Args:
            source: 'artlist' or 'motionarray'
            retry_errors: If True, include assets with ERROR status for reprocessing
        """
        if source.lower() == 'artlist':
            query = """
            WITH base AS (
              SELECT
                da.asset_id::string as asset_id,
                sf.filekey AS file_key,
                CASE WHEN sf.role = 'CORE' THEN 'wav' ELSE LOWER(sf.role) END AS file_format,
                0 as file_size,
                'artlist' as source,
                sf.createdat as created_at
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
                AND (af.asset_id IS NULL {retry_condition})  -- Unprocessed or errors
            ),
            deduplicated AS (
              SELECT asset_id, file_key, file_format, file_size, source
              FROM (
                SELECT
                  asset_id, file_key, file_format, file_size, source, created_at,
                  ROW_NUMBER() OVER (
                    PARTITION BY asset_id, file_format
                    ORDER BY created_at DESC, file_key
                  ) AS rn
                FROM base
              )
              WHERE rn = 1
            )
            SELECT asset_id, file_key, file_format, file_size, source
            FROM deduplicated
            ORDER BY asset_id
            """
            retry_condition = "OR af.processing_status = 'ERROR'" if retry_errors else ""
            query = query.format(retry_condition=retry_condition)
            
        elif source.lower() == 'motionarray':
            query = """
            WITH base AS (
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
                'motionarray' as source,
                c.created_at
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
                AND (af.asset_id IS NULL {retry_condition})  -- Unprocessed or errors
            ),
            deduplicated AS (
              SELECT asset_id, file_key, file_format, file_size, source
              FROM (
                SELECT
                  asset_id, file_key, file_format, file_size, source, created_at,
                  ROW_NUMBER() OVER (
                    PARTITION BY asset_id, file_format
                    ORDER BY created_at DESC, file_key
                  ) AS rn
                FROM base
              )
              WHERE rn = 1
            )
            SELECT asset_id, file_key, file_format, file_size, source
            FROM deduplicated
            ORDER BY asset_id
            """
            retry_condition = "OR af.processing_status = 'ERROR'" if retry_errors else ""
            query = query.format(retry_condition=retry_condition)
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
    
    def get_thread_snowflake(self) -> SnowflakeConnector:
        """Get or create thread-local Snowflake connection"""
        if not hasattr(self.thread_local, 'snowflake'):
            self.thread_local.snowflake = SnowflakeConnector()
            logger.info(f"🔗 [{threading.current_thread().name}] Created thread-local Snowflake connection")
        return self.thread_local.snowflake
    
    def cleanup_thread_temp_dir(self):
        """Clean up thread-specific temporary directory"""
        thread_id = threading.current_thread().ident
        if thread_id in self.temp_dirs:
            try:
                shutil.rmtree(self.temp_dirs[thread_id])
                del self.temp_dirs[thread_id]
            except Exception as e:
                logger.warning(f"⚠️  Failed to cleanup temp dir for thread {thread_id}: {e}")
    
    def cleanup_thread_connection(self):
        """Clean up thread-local Snowflake connection"""
        if hasattr(self.thread_local, 'snowflake'):
            try:
                self.thread_local.snowflake.close()
                delattr(self.thread_local, 'snowflake')
                logger.info(f"🔌 [{threading.current_thread().name}] Closed thread-local Snowflake connection")
            except Exception as e:
                logger.warning(f"⚠️  Failed to cleanup connection for thread: {e}")
    
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
        """Download audio file using Artlist/MotionArray API with enhanced validation"""
        try:
            download_url = self.get_download_url_from_api(file_key, source)
            if not download_url:
                logger.warning(f"❌ No download URL obtained for {file_key}")
                return None
            
            filename = Path(file_key).name
            if not any(filename.endswith(ext) for ext in ['.mp3', '.wav', '.flac', '.m4a']):
                filename += '.mp3'
            
            temp_file = temp_dir / filename
            
            import requests
            response = requests.get(download_url, stream=True, timeout=60)
            response.raise_for_status()
            
            # Check content type if available
            content_type = response.headers.get('content-type', '').lower()
            if content_type and not any(audio_type in content_type for audio_type in ['audio', 'mpeg', 'wav', 'flac']):
                logger.warning(f"⚠️  Unexpected content type for {file_key}: {content_type}")
            
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Enhanced file validation
            if not temp_file.exists():
                logger.warning(f"❌ Downloaded file does not exist: {file_key}")
                return None
            
            file_size = temp_file.stat().st_size
            
            # Check for minimum file size (1KB for audio files)
            if file_size == 0:
                logger.warning(f"❌ Downloaded file is empty: {file_key}")
                return None
            elif file_size < 1024:  # Less than 1KB
                logger.warning(f"❌ Downloaded file too small ({file_size} bytes): {file_key} - likely error response")
                # Let's check the content to see if it's an error message
                try:
                    with open(temp_file, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read(500)  # Read first 500 chars
                    if any(error_indicator in content.lower() for error_indicator in 
                           ['error', 'not found', 'access denied', 'forbidden', 'unauthorized', 'invalid']):
                        logger.warning(f"❌ File contains error message: {content[:200]}")
                except:
                    pass  # If we can't read as text, it might be binary but still too small
                return None
            elif file_size < 10240:  # Less than 10KB - suspicious for audio files
                logger.warning(f"⚠️  Downloaded file suspiciously small ({file_size} bytes): {file_key}")
            
            return temp_file
                
        except Exception as e:
            logger.warning(f"❌ Download error: {file_key} - {e}")
            return None
    
    def generate_fingerprint(self, file_path: Path) -> Optional[Tuple[float, str]]:
        """Generate fingerprint with robust error handling and validation"""
        try:
            # Pre-validation: Check file size again before processing
            file_size = file_path.stat().st_size
            if file_size < 1024:
                logger.warning(f"❌ File too small for fingerprinting ({file_size} bytes): {file_path.name}")
                return None
            
            # Attempt fingerprint generation
            duration, fingerprint = self.acoustid.fingerprint_file(str(file_path))
            
            # Validate results
            if duration is None or duration <= 0:
                logger.warning(f"❌ Invalid duration ({duration}): {file_path.name}")
                return None
            
            if fingerprint is None:
                logger.warning(f"❌ No fingerprint generated: {file_path.name}")
                return None
            
            # Convert fingerprint to string if needed
            if isinstance(fingerprint, bytes):
                fingerprint = fingerprint.decode('utf-8')
            
            # Validate fingerprint content
            if not fingerprint or len(fingerprint) < 10:
                logger.warning(f"❌ Fingerprint too short ({len(fingerprint)} chars): {file_path.name}")
                return None
            
            # Additional validation: duration should be reasonable for audio files
            if duration < 1.0:
                logger.warning(f"⚠️  Very short audio duration ({duration}s): {file_path.name}")
            elif duration > 3600:  # More than 1 hour
                logger.warning(f"⚠️  Very long audio duration ({duration}s): {file_path.name}")
            
            logger.debug(f"✅ Generated fingerprint: {file_path.name} - {duration}s, {len(fingerprint)} chars")
            return float(duration), fingerprint
                
        except Exception as e:
            error_msg = str(e)
            
            # Provide more specific error messages based on the exception
            if "could not be decoded" in error_msg.lower():
                logger.warning(f"❌ Audio decoding failed: {file_path.name} - file may be corrupted or in unsupported format")
            elif "no such file" in error_msg.lower():
                logger.warning(f"❌ File not found during fingerprinting: {file_path.name}")
            elif "permission denied" in error_msg.lower():
                logger.warning(f"❌ Permission denied accessing file: {file_path.name}")
            elif "timeout" in error_msg.lower():
                logger.warning(f"❌ Timeout during fingerprinting: {file_path.name}")
            else:
                logger.warning(f"❌ Fingerprint error: {file_path.name} - {error_msg}")
            
            return None
    
    def delete_existing_record(self, asset_id: str, file_key: str):
        """Delete existing record (used when retrying errors) (thread-safe)"""
        delete_sql = """
        DELETE FROM AI_DATA.AUDIO_FINGERPRINT 
        WHERE ASSET_ID = %(asset_id)s AND FILE_KEY = %(file_key)s
        """
        try:
            thread_snowflake = self.get_thread_snowflake()
            cursor = thread_snowflake.execute_query(delete_sql, {
                'asset_id': asset_id,
                'file_key': file_key
            })
            cursor.close()
        except Exception as e:
            logger.warning(f"⚠️  Failed to delete existing record: {asset_id} - {e}")
    
    def store_fingerprint(self, asset_id: str, file_key: str, format_ext: str, 
                         duration: float, fingerprint: str, file_size: int, source: str,
                         is_retry: bool = False):
        """Store fingerprint in Snowflake (thread-safe)"""
        # Delete old record if this is a retry
        if is_retry:
            self.delete_existing_record(asset_id, file_key)
        
        insert_sql = """
        INSERT INTO AI_DATA.AUDIO_FINGERPRINT 
        (ASSET_ID, FILE_KEY, FORMAT, DURATION, FINGERPRINT, FILE_SIZE, SOURCE, PROCESSING_STATUS)
        VALUES (%(asset_id)s, %(file_key)s, %(format)s, %(duration)s, %(fingerprint)s, %(file_size)s, %(source)s, 'SUCCESS')
        """
        
        try:
            # Use thread-local connection (reused across calls in same thread)
            thread_snowflake = self.get_thread_snowflake()
            cursor = thread_snowflake.execute_query(insert_sql, {
                'asset_id': asset_id,
                'file_key': file_key,
                'format': format_ext,
                'duration': duration,
                'fingerprint': fingerprint,
                'file_size': file_size,
                'source': source
            })
            cursor.close()  # Close cursor after use
        except Exception as e:
            logger.error(f"❌ Failed to store fingerprint: {asset_id} - {e}")
            raise
    
    def store_error(self, asset_id: str, file_key: str, format_ext: str, 
                   file_size: int, source: str, error_message: str, is_retry: bool = False):
        """Store processing error in Snowflake (thread-safe)"""
        # Delete old record if this is a retry
        if is_retry:
            self.delete_existing_record(asset_id, file_key)
        
        insert_sql = """
        INSERT INTO AI_DATA.AUDIO_FINGERPRINT 
        (ASSET_ID, FILE_KEY, FORMAT, FILE_SIZE, SOURCE, PROCESSING_STATUS, ERROR_MESSAGE)
        VALUES (%(asset_id)s, %(file_key)s, %(format)s, %(file_size)s, %(source)s, 'ERROR', %(error_message)s)
        """
        
        try:
            # Use thread-local connection (reused across calls in same thread)
            thread_snowflake = self.get_thread_snowflake()
            cursor = thread_snowflake.execute_query(insert_sql, {
                'asset_id': asset_id,
                'file_key': file_key,
                'format': format_ext,
                'file_size': file_size,
                'source': source,
                'error_message': error_message
            })
            cursor.close()  # Close cursor after use
        except Exception as e:
            logger.error(f"❌ Failed to store error: {asset_id} - {e}")
    
    def process_single_asset(self, asset_data: Dict, is_retry: bool = False) -> bool:
        """Process a single asset (thread-safe version) with enhanced error handling"""
        asset_id = asset_data['ASSET_ID']
        file_key = asset_data['FILE_KEY']
        file_format = asset_data.get('FILE_FORMAT', 'mp3')
        file_size = asset_data.get('FILE_SIZE', 0)
        source = asset_data.get('SOURCE', 'artlist')
        
        thread_name = threading.current_thread().name
        start_time = time.time()
        retry_msg = "[RETRY] " if is_retry else ""
        logger.info(f"🎵 [{thread_name}] {retry_msg}Processing: {asset_id} ({file_key})")
        
        try:
            temp_dir = self.get_thread_temp_dir()
            
            # Download file
            temp_file = self.download_audio_file(file_key, source, temp_dir)
            if not temp_file:
                error_msg = "Download failed - file not available or returned error response"
                self.store_error(asset_id, file_key, file_format, file_size, source, error_msg, is_retry)
                return False
            
            # Check if downloaded file is valid
            actual_file_size = temp_file.stat().st_size
            if actual_file_size < 1024:
                error_msg = f"Downloaded file too small ({actual_file_size} bytes) - likely API error response"
                self.store_error(asset_id, file_key, file_format, file_size, source, error_msg, is_retry)
                return False
            
            # Generate fingerprint
            result = self.generate_fingerprint(temp_file)
            if not result:
                error_msg = "Fingerprint generation failed - audio could not be decoded or file corrupted"
                self.store_error(asset_id, file_key, file_format, file_size, source, error_msg, is_retry)
                return False
            
            duration, fingerprint = result
            
            # Store in Snowflake
            self.store_fingerprint(asset_id, file_key, file_format, duration, fingerprint, actual_file_size, source, is_retry)
            
            # Clean up temp file
            temp_file.unlink()
            
            # Update stats (thread-safe)
            with self.stats_lock:
                self.stats['successful'] += 1
                self.stats['processed'] += 1
            processing_time = time.time() - start_time
            logger.info(f"✅ [{thread_name}] {retry_msg}Completed: {asset_id} ({processing_time:.1f}s processing, {duration:.1f}s audio, {actual_file_size:,} bytes)")
            return True
            
        except Exception as e:
            error_msg = f"Processing exception: {str(e)}"
            logger.error(f"❌ [{thread_name}] {retry_msg}Processing failed: {asset_id} - {e}")
            
            self.store_error(asset_id, file_key, file_format, file_size, source, error_msg, is_retry)
            
            # Update stats (thread-safe)
            with self.stats_lock:
                self.stats['failed'] += 1
                self.stats['processed'] += 1
            
            return False
        finally:
            self.cleanup_thread_temp_dir()
            # Don't cleanup connection here - let it be reused for next asset in same thread
    
    def process_assets_parallel(self, assets: List[Dict], is_retry: bool = False) -> Dict:
        """Process assets using parallel processing"""
        if not assets:
            logger.warning("⚠️  No assets to process")
            return {'processed': 0, 'successful': 0, 'failed': 0, 'total_time_minutes': 0}
        
        retry_msg = " (including ERROR retries)" if is_retry else ""
        logger.info(f"🚀 Starting parallel processing of {len(assets)} assets{retry_msg} with {self.max_workers} workers")
        
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
            future_to_asset = {executor.submit(self.process_single_asset, asset, is_retry): asset for asset in assets}
            
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
        
        # Clean up all thread-local connections after executor completes
        logger.info("Cleaning up thread-local connections...")
        # Note: ThreadPoolExecutor threads are already shut down at this point,
        # so connections are closed when threads terminate
        
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
    parser = argparse.ArgumentParser(description='Audio Fingerprint Processor with Parallel Processing')
    parser.add_argument('--source', choices=['artlist', 'motionarray'], 
                       help='Process ALL songs from specific source')
    parser.add_argument('--asset-ids', help='Comma-separated list of asset IDs to process')
    parser.add_argument('--workers', type=int, default=4, 
                       help='Number of parallel workers (default: 4)')
    parser.add_argument('--retry-errors', action='store_true',
                       help='Retry processing assets that previously failed with ERROR status')
    parser.add_argument('--stats', action='store_true', help='Show processing statistics')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.source and not args.asset_ids and not args.stats:
        parser.error("Must specify either --source, --asset-ids, or --stats")
    
    # Initialize processor
    try:
        processor = AudioFingerprintProcessor(max_workers=args.workers)
        processor.ensure_table_exists()
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
    
    # Get assets to process
    try:
        if args.source:
            retry_msg = " (including ERROR retries)" if args.retry_errors else ""
            logger.info(f"🎯 Processing ALL {args.source} songs{retry_msg}")
            assets = processor.get_all_assets_by_source(args.source, retry_errors=args.retry_errors)
        elif args.asset_ids:
            asset_ids = [aid.strip() for aid in args.asset_ids.split(',')]
            logger.info(f"🎯 Processing specific assets: {asset_ids}")
            assets = processor.get_asset_file_keys(asset_ids)
            # Note: asset_ids mode doesn't support retry_errors flag currently
        else:
            assets = []
        
        if not assets:
            logger.warning("⚠️  No assets found to process")
            return 0
        
        # Process assets
        results = processor.process_assets_parallel(assets, is_retry=args.retry_errors)
        
        # Print final results
        print(f"\n📊 Final Results:")
        print(f"   Total Assets: {len(assets)}")
        print(f"   Processed: {results['processed']}")
        print(f"   Successful: {results['successful']}")
        print(f"   Failed: {results['failed']}")
        success_rate = (results['successful']/results['processed']*100) if results['processed'] > 0 else 0.0
        print(f"   Success Rate: {success_rate:.1f}%")
        print(f"   Total Time: {results['total_time_minutes']:.1f} minutes")
        print(f"   Processing Rate: {results['rate_per_second']:.1f} assets/second")
        print(f"   Parallel Workers: {args.workers}")
        
        return 0 if results['failed'] == 0 else 1
        
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
