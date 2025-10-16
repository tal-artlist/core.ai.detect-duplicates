#!/usr/bin/env python3
"""
Smart Audio Duplicate Detector - MULTIPROCESSING OPTIMIZED 🚀

Loads ALL fingerprints from Snowflake and uses duration-based clustering 
with TRUE PARALLEL PROCESSING (ProcessPoolExecutor) to efficiently find duplicates.

🎯 DEFAULT BEHAVIOR: Writes results to JSONL file (fast, cost-effective)
📤 UPLOAD MODE: Load file and upload to Snowflake in batches (--load-and-upload)
🔄 RESUME SUPPORT: Automatically resumes from checkpoint if interrupted
⚠️  ERROR TRACKING: Failed comparisons saved to *_errors.jsonl for retry

Prioritizes cross-source comparisons (artlist ↔ motionarray).

Usage:
    # Step 1: Detect duplicates and write to file (auto-generates timestamped filename)
    python duplicate_detector.py --mode cross-source --workers 12 --duration-tolerance 1.0
    
    # If interrupted (Ctrl+C), just run the same command again - it will resume!
    # Progress is saved in a .checkpoint file every 10 clusters
    
    # Step 1 (with custom filename):
    python duplicate_detector.py --mode cross-source --output results/duplicates.jsonl
    
    # If errors occur, they're saved to: results/duplicates_errors.jsonl
    # Analyze and retry: python duplicate_detector_retry_errors.py results/duplicates_errors.jsonl --analyze-only
    
    # Step 2: Load results from file and upload to Snowflake in batches
    python duplicate_detector.py --load-and-upload duplicate_results_cross-source_20250116_143022.jsonl
    
    # Show statistics from Snowflake
    python duplicate_detector.py --stats
    
    # Start fresh (ignore checkpoint):
    python duplicate_detector.py --mode cross-source --no-resume
"""

import os
import sys
import logging
import argparse
import tempfile
import shutil
import json
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
import subprocess
import time
import platform
from concurrent.futures import ProcessPoolExecutor, as_completed
from threading import Lock
from multiprocessing import cpu_count, set_start_method, get_start_method
import multiprocessing

# Set multiprocessing start method to 'spawn' for better library compatibility
# This MUST be done before any other multiprocessing code
try:
    if get_start_method(allow_none=True) is None:
        set_start_method('spawn', force=True)
except RuntimeError:
    pass  # Already set
import ctypes
from datetime import datetime

# Platform-specific environment setup
SYSTEM = platform.system()

# Auto-restart with correct environment if needed (cross-platform)
if SYSTEM == 'Darwin':  # macOS
    if 'DYLD_LIBRARY_PATH' not in os.environ or '/opt/homebrew/lib' not in os.environ.get('DYLD_LIBRARY_PATH', ''):
        env = os.environ.copy()
        env['DYLD_LIBRARY_PATH'] = '/opt/homebrew/lib:' + env.get('DYLD_LIBRARY_PATH', '')
        result = subprocess.run([sys.executable] + sys.argv, env=env)
        sys.exit(result.returncode)
elif SYSTEM == 'Linux':
    # Linux may need LD_LIBRARY_PATH for custom installs
    pass

try:
    import acoustid
    from snowflake_utils import SnowflakeConnector
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure to install requirements: pip install -r requirements.txt")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# MULTIPROCESSING WORKER FUNCTIONS (must be at module level for pickling)
# ============================================================================

def setup_chromaprint_for_worker():
    """Setup chromaprint for each worker process - lightweight version"""
    # Don't try to load libraries - just use acoustid's built-in functionality
    # The acoustid module will handle this automatically
    return True

def process_comparison_worker(work_data):
    """
    Worker function for multiprocessing - compares a single pair of songs.
    Must be at module level for pickle serialization.
    
    Args:
        work_data: Tuple of (song1_dict, song2_dict, pair_id)
    
    Returns:
        Dict with comparison results
    """
    try:
        song1, song2, pair_id = work_data
        
        # Skip if same file
        if song1['file_key'] == song2['file_key']:
            return {
                'pair_id': pair_id,
                'skipped': True,
                'reason': 'same_file'
            }
        
        # Convert fingerprints to bytes
        fp1_bytes = song1['fingerprint'].encode('utf-8')
        fp2_bytes = song2['fingerprint'].encode('utf-8')
        
        # Compare fingerprints (no lock needed - each process is independent!)
        start_time = time.perf_counter()
        similarity = acoustid.compare_fingerprints(
            (song1['duration'], fp1_bytes),
            (song2['duration'], fp2_bytes)
        )
        comp_time = time.perf_counter() - start_time
        
        if similarity is None:
            return {
                'pair_id': pair_id,
                'success': False,
                'error': 'comparison_returned_none'
            }
        
        # Classify duplicate type
        same_format = song1['format'] == song2['format']
        same_source = song1['source'] == song2['source']
        
        if similarity >= 0.95:
            if same_format and same_source:
                dup_type = "IDENTICAL"
            elif same_source:
                dup_type = "SAME_CONTENT_DIFF_FORMAT"
            else:
                dup_type = "CROSS_SOURCE_IDENTICAL"
        elif similarity >= 0.80:
            dup_type = "HIGH_SIMILARITY_CROSS_SOURCE" if not same_source else "HIGH_SIMILARITY_SAME_SOURCE"
        elif similarity >= 0.60:
            dup_type = "RELATED_VERSIONS"
        else:
            dup_type = "LOW_SIMILARITY"
        
        # Build result record
        return {
            'pair_id': pair_id,
            'success': True,
            'similarity': float(similarity),
            'duplicate_type': dup_type,
            'comp_time': comp_time,
            'record': {
                'asset_id_1': song1['asset_id'],
                'asset_id_2': song2['asset_id'],
                'is_same_asset': song1['asset_id'] == song2['asset_id'],
                'similarity': float(similarity),
                'duplicate_type': dup_type,
                'file_key_1': song1['file_key'],
                'format_1': song1['format'],
                'source_1': song1['source'],
                'duration_1': song1['duration'],
                'file_key_2': song2['file_key'],
                'format_2': song2['format'],
                'source_2': song2['source'],
                'duration_2': song2['duration'],
                'duration_diff': abs(song1['duration'] - song2['duration'])
            }
        }
        
    except Exception as e:
        return {
            'pair_id': pair_id,
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__
        }

# ============================================================================

class DuplicateDetector:
    """Smart duplicate detector using duration-based clustering with parallel processing"""
    
    def __init__(self, max_workers: int = 4, batch_size: int = 10000, output_file: Optional[str] = None):
        """Initialize the duplicate detector
        
        Args:
            max_workers: Number of parallel workers
            batch_size: Batch size for Snowflake writes
            output_file: If provided, write results to file instead of Snowflake
        """
        self.snowflake = SnowflakeConnector()
        self.setup_chromaprint()
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.output_file = output_file
        self.stats_lock = Lock()
        # No comparison_lock needed with ProcessPoolExecutor! 🚀
        self.stats = {
            'comparisons': 0,
            'duplicates': 0,
            'skipped': 0,
            'errors': 0,
            'start_time': None
        }
        # Batch writing buffer (thread-safe)
        self.batch_lock = Lock()
        self.duplicate_batch = []
        
        # File output buffer (if using file mode)
        if self.output_file:
            self.file_lock = Lock()
            self.file_buffer = []
            self.checkpoint_file = self.output_file + '.checkpoint'
            self.checkpoint_lock = Lock()
            self.completed_clusters = set()  # Track completed cluster indices
            
            # Error tracking (separate file for failed comparisons)
            self.error_file = self.output_file.replace('.jsonl', '_errors.jsonl')
            self.error_lock = Lock()
            self.error_buffer = []
        
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
                    'libchromaprint.so.1',
                    'libchromaprint.so',
                    '/lib/x86_64-linux-gnu/libchromaprint.so.1',
                    '/usr/lib/x86_64-linux-gnu/libchromaprint.so.1',
                    '/usr/lib/libchromaprint.so.1'
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
            
            # Find fpcalc binary
            fpcalc_path = shutil.which('fpcalc')
            if not fpcalc_path:
                if SYSTEM == 'Darwin':
                    candidate_paths = ['/opt/homebrew/bin/fpcalc', '/usr/local/bin/fpcalc']
                else:
                    candidate_paths = ['/usr/bin/fpcalc', '/usr/local/bin/fpcalc']
                
                for path in candidate_paths:
                    if os.path.exists(path):
                        fpcalc_path = path
                        break
            
            if not fpcalc_path:
                raise RuntimeError(f"Could not find fpcalc binary for {SYSTEM}")
            
            os.environ['FPCALC_COMMAND'] = fpcalc_path
            acoustid.FPCALC_COMMAND = fpcalc_path
            
            logger.info(f"✅ Chromaprint setup successful on {SYSTEM} (fpcalc: {fpcalc_path})")
            return acoustid
        except Exception as e:
            logger.error(f"❌ Chromaprint setup failed on {SYSTEM}: {e}")
            raise
    
    def ensure_duplicates_table_exists(self):
        """Create AUDIO_DETECTED_DUPLICATES table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS AI_DATA.AUDIO_DETECTED_DUPLICATES (
            ID NUMBER AUTOINCREMENT PRIMARY KEY,
            ASSET_ID_1 VARCHAR(50) NOT NULL,
            ASSET_ID_2 VARCHAR(50) NOT NULL,
            IS_SAME_ASSET BOOLEAN NOT NULL,
            SIMILARITY FLOAT NOT NULL,
            DUPLICATE_TYPE VARCHAR(50),
            FILE_KEY_1 VARCHAR(500) NOT NULL,
            FORMAT_1 VARCHAR(10),
            SOURCE_1 VARCHAR(20),
            DURATION_1 FLOAT,
            FILE_KEY_2 VARCHAR(500) NOT NULL,
            FORMAT_2 VARCHAR(10),
            SOURCE_2 VARCHAR(20),
            DURATION_2 FLOAT,
            DURATION_DIFF FLOAT,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            UNIQUE(ASSET_ID_1, FILE_KEY_1, ASSET_ID_2, FILE_KEY_2)
        )
        """
        
        try:
            self.snowflake.execute_query(create_table_sql)
            logger.info("✅ AUDIO_DETECTED_DUPLICATES table ready")
        except Exception as e:
            logger.error(f"❌ Failed to create duplicates table: {e}")
            raise
    
    def load_all_fingerprints(self) -> List[Dict]:
        """Load ALL fingerprints from Snowflake (no limit)"""
        query = """
        SELECT 
            ASSET_ID,
            FILE_KEY,
            FORMAT,
            DURATION,
            FINGERPRINT,
            FILE_SIZE,
            SOURCE
        FROM AI_DATA.AUDIO_FINGERPRINT 
        WHERE PROCESSING_STATUS = 'SUCCESS'
            AND FINGERPRINT IS NOT NULL
            AND DURATION > 0
        ORDER BY DURATION, SOURCE, ASSET_ID
        """
        
        try:
            logger.info("📥 Loading ALL fingerprints from Snowflake (this may take a minute)...")
            start_time = time.time()
            cursor = self.snowflake.execute_query(query)
            
            fingerprints = []
            for row in cursor:
                fingerprints.append({
                    'asset_id': row[0],
                    'file_key': row[1],
                    'format': row[2],
                    'duration': float(row[3]),
                    'fingerprint': row[4],
                    'file_size': int(row[5]) if row[5] else 0,
                    'source': row[6]
                })
            
            cursor.close()
            load_time = time.time() - start_time
            
            # Count by source
            source_counts = defaultdict(int)
            for fp in fingerprints:
                source_counts[fp['source']] += 1
            
            logger.info(f"✅ Loaded {len(fingerprints):,} fingerprints in {load_time:.1f}s")
            for source, count in sorted(source_counts.items()):
                logger.info(f"   {source}: {count:,} fingerprints")
            
            return fingerprints
            
        except Exception as e:
            logger.error(f"❌ Failed to load fingerprints: {e}")
            raise
    
    def check_if_duplicate_exists(self, asset_id_1: str, file_key_1: str, 
                                  asset_id_2: str, file_key_2: str) -> bool:
        """Check if this duplicate pair already exists in the database"""
        query = """
        SELECT COUNT(*) 
        FROM AI_DATA.AUDIO_DETECTED_DUPLICATES
        WHERE (
            (ASSET_ID_1 = %(aid1)s AND FILE_KEY_1 = %(fk1)s AND 
             ASSET_ID_2 = %(aid2)s AND FILE_KEY_2 = %(fk2)s)
            OR
            (ASSET_ID_1 = %(aid2)s AND FILE_KEY_1 = %(fk2)s AND 
             ASSET_ID_2 = %(aid1)s AND FILE_KEY_2 = %(fk1)s)
        )
        """
        try:
            cursor = self.snowflake.execute_query(query, {
                'aid1': asset_id_1, 'fk1': file_key_1,
                'aid2': asset_id_2, 'fk2': file_key_2
            })
            count = cursor.fetchone()[0]
            cursor.close()
            return count > 0
        except Exception as e:
            logger.warning(f"⚠️  Failed to check duplicate existence: {e}")
            return False
    
    def cluster_by_duration(self, songs: List[Dict], tolerance: float = 5.0) -> List[List[Dict]]:
        """Cluster songs by duration with tolerance"""
        # Sort by duration (already sorted from query, but just to be sure)
        songs_sorted = sorted(songs, key=lambda x: x['duration'])
        
        clusters = []
        current_cluster = []
        
        for song in songs_sorted:
            if not current_cluster:
                # Start new cluster
                current_cluster = [song]
            else:
                # Check if song fits in current cluster
                cluster_min_duration = min(s['duration'] for s in current_cluster)
                cluster_max_duration = max(s['duration'] for s in current_cluster)
                
                if (song['duration'] - cluster_min_duration <= tolerance and 
                    song['duration'] - cluster_max_duration <= tolerance):
                    # Add to current cluster
                    current_cluster.append(song)
                else:
                    # Start new cluster
                    if len(current_cluster) >= 2:
                        clusters.append(current_cluster)
                    current_cluster = [song]
        
        # Don't forget the last cluster
        if len(current_cluster) >= 2:
            clusters.append(current_cluster)
        
        logger.info(f"📊 Created {len(clusters)} duration clusters (tolerance: {tolerance}s)")
        total_songs_in_clusters = sum(len(cluster) for cluster in clusters)
        logger.info(f"📊 {total_songs_in_clusters} songs in clusters (potential duplicates)")
        
        return clusters
    
    def filter_cluster_by_mode(self, cluster: List[Dict], mode: str) -> List[Tuple[Dict, Dict]]:
        """
        Filter cluster to generate comparison pairs based on mode.
        
        Args:
            cluster: List of songs in the same duration cluster
            mode: 'cross-source', 'same-source', or 'all'
        
        Returns:
            List of (song1, song2) tuples to compare
        """
        pairs = []
        
        for i in range(len(cluster)):
            for j in range(i + 1, len(cluster)):
                song1, song2 = cluster[i], cluster[j]
                
                # Skip if same file
                if song1['file_key'] == song2['file_key']:
                    continue
                
                # Filter by mode
                same_source = song1['source'] == song2['source']
                
                if mode == 'cross-source' and same_source:
                    continue
                elif mode == 'same-source' and not same_source:
                    continue
                # 'all' mode: include everything
                
                pairs.append((song1, song2))
        
        return pairs
    
    def classify_duplicate_type(self, song1: Dict, song2: Dict, similarity: float) -> str:
        """Classify the type of duplicate based on similarity"""
        same_format = song1['format'] == song2['format']
        same_source = song1['source'] == song2['source']
        
        if similarity >= 0.95:
            if same_format and same_source:
                return "IDENTICAL"
            elif same_source:
                return "SAME_CONTENT_DIFF_FORMAT"
            else:
                return "CROSS_SOURCE_IDENTICAL"
        elif similarity >= 0.80:
            if same_source:
                return "HIGH_SIMILARITY_SAME_SOURCE"
            else:
                return "HIGH_SIMILARITY_CROSS_SOURCE"
        elif similarity >= 0.60:
            return "RELATED_VERSIONS"
        else:
            return "LOW_SIMILARITY"
    
    def compare_fingerprints(self, song1: Dict, song2: Dict) -> Optional[float]:
        """Compare two fingerprints and return similarity score (thread-safe)"""
        try:
            # Convert string fingerprints to bytes (required by acoustid.compare_fingerprints)
            fp1_bytes = song1['fingerprint'].encode('utf-8')
            fp2_bytes = song2['fingerprint'].encode('utf-8')
            
            # CRITICAL: acoustid/chromaprint is NOT thread-safe!
            # Must serialize all comparisons with a lock
            with self.comparison_lock:
                similarity = acoustid.compare_fingerprints(
                    (song1['duration'], fp1_bytes),
                    (song2['duration'], fp2_bytes)
                )
            
            return float(similarity)
        except Exception as e:
            logger.warning(f"⚠️  Fingerprint comparison failed: {e}")
            return None
    
    def flush_duplicate_batch(self):
        """Flush accumulated duplicate records to Snowflake (thread-safe)"""
        with self.batch_lock:
            if not self.duplicate_batch:
                return
            
            batch_to_write = self.duplicate_batch.copy()
            self.duplicate_batch.clear()
        
        if not batch_to_write:
            return
        
        insert_sql = """
        INSERT INTO AI_DATA.AUDIO_DETECTED_DUPLICATES 
        (ASSET_ID_1, ASSET_ID_2, IS_SAME_ASSET, SIMILARITY, DUPLICATE_TYPE,
         FILE_KEY_1, FORMAT_1, SOURCE_1, DURATION_1,
         FILE_KEY_2, FORMAT_2, SOURCE_2, DURATION_2, DURATION_DIFF)
        VALUES (%(asset_id_1)s, %(asset_id_2)s, %(is_same_asset)s, %(similarity)s, %(duplicate_type)s,
                %(file_key_1)s, %(format_1)s, %(source_1)s, %(duration_1)s,
                %(file_key_2)s, %(format_2)s, %(source_2)s, %(duration_2)s, %(duration_diff)s)
        """
        
        try:
            logger.info(f"💾 Flushing {len(batch_to_write)} duplicate records to Snowflake...")
            cursor = self.snowflake.conn.cursor()
            cursor.executemany(insert_sql, batch_to_write)
            cursor.close()
            logger.info(f"✅ Successfully wrote {len(batch_to_write)} duplicate records")
        except Exception as e:
            logger.error(f"❌ Failed to flush duplicate batch: {e}")
            raise
    
    def flush_file_buffer(self):
        """Flush accumulated records to file (thread-safe)"""
        if not self.output_file:
            return
        
        with self.file_lock:
            if not self.file_buffer:
                return
            
            buffer_to_write = self.file_buffer.copy()
            self.file_buffer.clear()
        
        if not buffer_to_write:
            return
        
        try:
            # Append to JSON lines file (one JSON object per line)
            with open(self.output_file, 'a') as f:
                for record in buffer_to_write:
                    f.write(json.dumps(record) + '\n')
            
            logger.debug(f"💾 Wrote {len(buffer_to_write)} records to {self.output_file}")
        except Exception as e:
            logger.error(f"❌ Failed to write to file: {e}")
            raise
    
    def flush_error_buffer(self):
        """Flush accumulated error records to error file (thread-safe)"""
        if not self.output_file:
            return
        
        with self.error_lock:
            if not self.error_buffer:
                return
            
            buffer_to_write = self.error_buffer.copy()
            self.error_buffer.clear()
        
        if not buffer_to_write:
            return
        
        try:
            # Append to JSON lines file (one JSON object per line)
            with open(self.error_file, 'a') as f:
                for record in buffer_to_write:
                    f.write(json.dumps(record) + '\n')
            
            logger.debug(f"💾 Wrote {len(buffer_to_write)} error records to {self.error_file}")
        except Exception as e:
            logger.error(f"❌ Failed to write errors to file: {e}")
            raise
    
    def load_checkpoint(self) -> Dict:
        """Load checkpoint from file if it exists"""
        if not self.output_file or not os.path.exists(self.checkpoint_file):
            return {'completed_clusters': [], 'stats': {}}
        
        try:
            with open(self.checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
            
            self.completed_clusters = set(checkpoint.get('completed_clusters', []))
            logger.info(f"📥 Loaded checkpoint: {len(self.completed_clusters)} clusters already completed")
            
            return checkpoint
        except Exception as e:
            logger.warning(f"⚠️  Failed to load checkpoint: {e}")
            return {'completed_clusters': [], 'stats': {}}
    
    def save_checkpoint(self, current_cluster_idx: int, total_clusters: int, force: bool = False):
        """Save checkpoint to file (thread-safe, throttled)"""
        if not self.output_file:
            return
        
        # Save every 10 clusters or when forced (at the end)
        if not force and current_cluster_idx % 10 != 0:
            return
        
        with self.checkpoint_lock:
            try:
                checkpoint = {
                    'version': '1.0',
                    'timestamp': datetime.now().isoformat(),
                    'output_file': self.output_file,
                    'completed_clusters': sorted(list(self.completed_clusters)),
                    'total_clusters': total_clusters,
                    'progress_pct': (len(self.completed_clusters) / total_clusters * 100) if total_clusters > 0 else 0,
                    'stats': {
                        'comparisons': self.stats['comparisons'],
                        'duplicates': self.stats['duplicates'],
                        'skipped': self.stats['skipped']
                    }
                }
                
                # Write checkpoint atomically (write to temp file, then rename)
                temp_checkpoint = self.checkpoint_file + '.tmp'
                with open(temp_checkpoint, 'w') as f:
                    json.dump(checkpoint, f, indent=2)
                
                # Atomic rename
                os.replace(temp_checkpoint, self.checkpoint_file)
                
                logger.debug(f"💾 Checkpoint saved: {len(self.completed_clusters)}/{total_clusters} clusters")
            except Exception as e:
                logger.warning(f"⚠️  Failed to save checkpoint: {e}")
    
    def cleanup_checkpoint(self):
        """Remove checkpoint file after successful completion"""
        if self.output_file and os.path.exists(self.checkpoint_file):
            try:
                os.remove(self.checkpoint_file)
                logger.info(f"🧹 Cleaned up checkpoint file")
            except Exception as e:
                logger.warning(f"⚠️  Failed to cleanup checkpoint: {e}")
    
    def store_duplicate(self, song1: Dict, song2: Dict, similarity: float, duplicate_type: str):
        """Store duplicate pair in buffer (file or Snowflake batch) (thread-safe)"""
        record = {
            'asset_id_1': song1['asset_id'],
            'asset_id_2': song2['asset_id'],
            'is_same_asset': song1['asset_id'] == song2['asset_id'],
            'similarity': similarity,
            'duplicate_type': duplicate_type,
            'file_key_1': song1['file_key'],
            'format_1': song1['format'],
            'source_1': song1['source'],
            'duration_1': song1['duration'],
            'file_key_2': song2['file_key'],
            'format_2': song2['format'],
            'source_2': song2['source'],
            'duration_2': song2['duration'],
            'duration_diff': abs(song1['duration'] - song2['duration'])
        }
        
        if self.output_file:
            # Write to file buffer
            with self.file_lock:
                self.file_buffer.append(record)
                should_flush = len(self.file_buffer) >= 1000  # Flush file buffer more frequently
            
            if should_flush:
                self.flush_file_buffer()
        else:
            # Write to Snowflake batch buffer
            with self.batch_lock:
                self.duplicate_batch.append(record)
                should_flush = len(self.duplicate_batch) >= self.batch_size
            
            if should_flush:
                self.flush_duplicate_batch()
        
        logger.debug(f"✅ Queued duplicate: {song1['asset_id']} <-> {song2['asset_id']} ({similarity:.3f})")
    
    def store_error(self, song1: Dict, song2: Dict, error_type: str, error_message: str):
        """Store failed comparison in error buffer (thread-safe)"""
        if not self.output_file:
            # Only track errors when writing to file (for retry capability)
            return
        
        error_record = {
            'error_type': error_type,
            'error_message': error_message,
            'timestamp': datetime.now().isoformat(),
            'asset_id_1': song1['asset_id'],
            'file_key_1': song1['file_key'],
            'format_1': song1['format'],
            'source_1': song1['source'],
            'duration_1': song1['duration'],
            'asset_id_2': song2['asset_id'],
            'file_key_2': song2['file_key'],
            'format_2': song2['format'],
            'source_2': song2['source'],
            'duration_2': song2['duration'],
            'duration_diff': abs(song1['duration'] - song2['duration'])
        }
        
        with self.error_lock:
            self.error_buffer.append(error_record)
            should_flush = len(self.error_buffer) >= 100  # Flush error buffer periodically
        
        if should_flush:
            self.flush_error_buffer()
        
        logger.debug(f"⚠️  Queued error: {song1['asset_id']} <-> {song2['asset_id']} | {error_type}: {error_message}")
    
    def compare_and_store_pair(self, pair: Tuple[Dict, Dict], similarity_threshold: float = 0.0) -> Optional[Dict]:
        """
        Compare a single pair of songs and store if they're similar.
        
        Args:
            pair: (song1, song2) tuple
            similarity_threshold: Minimum similarity to store (0.0 = store all)
        
        Returns:
            Duplicate dict if found, None otherwise
        """
        song1, song2 = pair
        
        try:
            # Check if already processed
            if self.check_if_duplicate_exists(song1['asset_id'], song1['file_key'], 
                                             song2['asset_id'], song2['file_key']):
                with self.stats_lock:
                    self.stats['skipped'] += 1
                return None
            
            # Compare fingerprints
            similarity = self.compare_fingerprints(song1, song2)
            
            # Check if comparison failed
            if similarity is None:
                # Record the error for retry
                self.store_error(song1, song2, 'COMPARISON_FAILED', 'Fingerprint comparison returned None')
                with self.stats_lock:
                    self.stats['errors'] += 1
                return None
            
            # Update comparison count
            with self.stats_lock:
                self.stats['comparisons'] += 1
            
            if similarity >= similarity_threshold:
                duplicate_type = self.classify_duplicate_type(song1, song2, similarity)
                
                # Store in database
                self.store_duplicate(song1, song2, similarity, duplicate_type)
                
                with self.stats_lock:
                    self.stats['duplicates'] += 1
                
                logger.info(f"🔍 Duplicate: {song1['asset_id']} ({song1['source']}/{song1['format']}) <-> "
                          f"{song2['asset_id']} ({song2['source']}/{song2['format']}) | "
                          f"Similarity: {similarity:.3f} | {duplicate_type}")
                
                return {
                    'song1': song1,
                    'song2': song2,
                    'similarity': similarity,
                    'type': duplicate_type
                }
            
            return None
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Failed to compare pair: {error_msg}")
            # Record the error for retry
            self.store_error(song1, song2, 'EXCEPTION', error_msg)
            with self.stats_lock:
                self.stats['errors'] += 1
            return None
    
    def find_duplicates_in_cluster_parallel(self, cluster: List[Dict], mode: str, 
                                           similarity_threshold: float = 0.0) -> int:
        """
        Find duplicates within a single duration cluster using MULTIPROCESSING (OPTIMIZED! 🚀).
        
        Args:
            cluster: List of songs in the same duration cluster
            mode: 'cross-source', 'same-source', or 'all'
            similarity_threshold: Minimum similarity to store (0.0 = store all)
        
        Returns:
            Number of duplicates found
        """
        # Generate pairs based on mode
        pairs = self.filter_cluster_by_mode(cluster, mode)
        
        if not pairs:
            return 0
        
        duplicates_found = 0
        
        # Prepare work data for worker processes (song1, song2, pair_id)
        work_items = []
        for idx, (song1, song2) in enumerate(pairs):
            # Check if already processed (to save on useless comparisons)
            if self.check_if_duplicate_exists(song1['asset_id'], song1['file_key'], 
                                             song2['asset_id'], song2['file_key']):
                with self.stats_lock:
                    self.stats['skipped'] += 1
                continue
            
            work_items.append((song1, song2, idx))
        
        if not work_items:
            return 0
        
        # Process pairs in parallel with MULTIPROCESSING 🚀
        # Each process runs independently, no GIL, pure parallel CPU power!
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(process_comparison_worker, work): work[2] 
                      for work in work_items}
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    
                    if result.get('skipped'):
                        with self.stats_lock:
                            self.stats['skipped'] += 1
                        continue
                    
                    if not result.get('success'):
                        # Handle error
                        error = result.get('error', 'unknown')
                        work_item = work_items[result['pair_id']]
                        song1, song2 = work_item[0], work_item[1]
                        
                        # Store error for retry
                        self.store_error(song1, song2, 'COMPARISON_FAILED', error)
                        
                        with self.stats_lock:
                            self.stats['errors'] += 1
                        continue
                    
                    # Successful comparison
                    similarity = result['similarity']
                    
                    with self.stats_lock:
                        self.stats['comparisons'] += 1
                        comparisons_count = self.stats['comparisons']
                        current_time = time.time()
                        time_since_last_progress = current_time - self.stats['last_progress_time']
                    
                    # Progress update every 1000 comparisons OR every 5 seconds
                    show_progress = (comparisons_count % 1000 == 0) or (time_since_last_progress >= 5.0)
                    
                    if show_progress:
                        with self.stats_lock:
                            elapsed = time.time() - self.stats['start_time']
                            rate = self.stats['comparisons'] / elapsed if elapsed > 0 else 0
                            self.stats['last_progress_time'] = time.time()
                            
                            logger.info(f"⚡ {comparisons_count:,} comparisons | "
                                      f"{self.stats['duplicates']:,} duplicates | "
                                      f"Avg: {rate:.0f} comp/sec")
                    
                    # Check threshold and store if needed
                    if similarity >= similarity_threshold:
                        record = result['record']
                        
                        # Store duplicate
                        if self.output_file:
                            # Write to file
                            with self.file_lock:
                                self.file_buffer.append(record)
                                if len(self.file_buffer) >= self.batch_size:
                                    self.flush_file_buffer()
                        else:
                            # Store in database
                            with self.batch_lock:
                                self.duplicate_batch.append(record)
                                if len(self.duplicate_batch) >= self.batch_size:
                                    self.flush_database_batch()
                        
                        with self.stats_lock:
                            self.stats['duplicates'] += 1
                        
                        duplicates_found += 1
                        
                        # Log if high similarity
                        if similarity >= 0.60:
                            work_item = work_items[result['pair_id']]
                            song1, song2 = work_item[0], work_item[1]
                            logger.info(f"🔍 Duplicate: {song1['asset_id']} ({song1['source']}/{song1['format']}) <-> "
                                      f"{song2['asset_id']} ({song2['source']}/{song2['format']}) | "
                                      f"Similarity: {similarity:.3f} | {result['duplicate_type']}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to process comparison result: {e}")
                    with self.stats_lock:
                        self.stats['errors'] += 1
        
        return duplicates_found
    
    def detect_duplicates(self, mode: str = 'cross-source', 
                         similarity_threshold: float = 0.0,
                         duration_tolerance: float = 5.0,
                         resume: bool = True) -> Dict:
        """
        Main duplicate detection process with parallel processing.
        
        Args:
            mode: 'cross-source' (artlist ↔ motionarray), 'same-source', or 'all'
            similarity_threshold: Minimum similarity to store (0.0 = store all for later analysis)
            duration_tolerance: Duration clustering tolerance in seconds
            resume: If True and checkpoint exists, resume from checkpoint
        
        Returns:
            Dict with processing statistics
        """
        logger.info(f"🚀 Starting duplicate detection")
        logger.info(f"   Mode: {mode}")
        logger.info(f"   Similarity threshold: {similarity_threshold} (0.0 = store all)")
        logger.info(f"   Duration tolerance: ±{duration_tolerance}s")
        logger.info(f"   Parallel workers: {self.max_workers}")
        
        # Load checkpoint if resuming
        checkpoint = {}
        if resume and self.output_file:
            checkpoint = self.load_checkpoint()
            if self.completed_clusters:
                logger.info(f"🔄 RESUMING from checkpoint: {len(self.completed_clusters)} clusters already completed")
        
        start_time = time.time()
        
        # Initialize stats
        with self.stats_lock:
            self.stats = {
                'comparisons': 0,
                'duplicates': 0,
                'skipped': 0,
                'errors': 0,
                'start_time': start_time,
                'last_progress_time': start_time  # Track last progress log time
            }
        
        # Ensure tables exist
        self.ensure_duplicates_table_exists()
        
        # Load ALL fingerprints
        songs = self.load_all_fingerprints()
        
        if len(songs) < 2:
            logger.warning("⚠️  Not enough songs to compare")
            return {'duplicates': 0, 'comparisons': 0, 'clusters': 0, 'songs': 0}
        
        # Cluster by duration
        logger.info(f"🔄 Clustering {len(songs):,} songs by duration (±{duration_tolerance}s)...")
        clusters = self.cluster_by_duration(songs, duration_tolerance)
        
        if not clusters:
            logger.warning("⚠️  No duration clusters found")
            return {'duplicates': 0, 'comparisons': 0, 'clusters': 0, 'songs': len(songs)}
        
        # Start processing (pair counting removed - was too slow and unnecessary)
        logger.info(f"🚀 Processing {len(clusters):,} clusters...")
        
        # Create output files immediately (so user knows they exist)
        if self.output_file:
            # Touch the output file
            open(self.output_file, 'a').close()
            logger.info(f"📝 Output file created: {self.output_file}")
            
            # Touch the error file
            open(self.error_file, 'a').close()
            logger.info(f"📝 Error tracking file: {self.error_file}")
        
        # Count clusters to skip
        clusters_to_skip = len(self.completed_clusters)
        if clusters_to_skip > 0:
            logger.info(f"⏭️  Skipping {clusters_to_skip} already completed clusters")
        
        # Process each cluster
        for i, cluster in enumerate(clusters, 1):
            # Skip if already completed (when resuming)
            if i in self.completed_clusters:
                logger.debug(f"⏭️  Skipping cluster {i} (already completed)")
                continue
            
            cluster_pairs = self.filter_cluster_by_mode(cluster, mode)
            
            if not cluster_pairs:
                # Mark as completed even if no pairs
                self.completed_clusters.add(i)
                self.save_checkpoint(i, len(clusters))
                continue
            
            # Log cluster start
            logger.info(f"🔍 Cluster {i}/{len(clusters)} | Duration: {cluster[0]['duration']:.1f}s | "
                       f"Songs: {len(cluster)} | Pairs: {len(cluster_pairs):,}")
            
            # Track stats before processing
            with self.stats_lock:
                comparisons_before = self.stats['comparisons']
                duplicates_before = self.stats['duplicates']
                errors_before = self.stats['errors']
            
            cluster_start_time = time.time()
            
            # Process cluster in parallel
            self.find_duplicates_in_cluster_parallel(cluster, mode, similarity_threshold)
            
            # Show cluster completion
            cluster_time = time.time() - cluster_start_time
            with self.stats_lock:
                cluster_comps = self.stats['comparisons'] - comparisons_before
                cluster_dups = self.stats['duplicates'] - duplicates_before
                cluster_errs = self.stats['errors'] - errors_before
            
            cluster_rate = cluster_comps / cluster_time if cluster_time > 0 else 0
            logger.info(f"   ✅ Done: {cluster_comps:,} comparisons in {cluster_time:.1f}s ({cluster_rate:.0f} comp/sec) | "
                       f"{cluster_dups} duplicates, {cluster_errs} errors")
            
            # Mark cluster as completed
            self.completed_clusters.add(i)
            
            # Save checkpoint periodically
            self.save_checkpoint(i, len(clusters))
            
            # Overall progress summary (every 100 clusters)
            if i % 100 == 0 or i == len(clusters):
                with self.stats_lock:
                    elapsed = time.time() - self.stats['start_time']
                    rate = self.stats['comparisons'] / elapsed if elapsed > 0 else 0
                    completed = len(self.completed_clusters)
                    
                    logger.info(f"\n" + "="*80)
                    logger.info(f"📊 OVERALL PROGRESS: {completed}/{len(clusters)} clusters ({completed/len(clusters)*100:.1f}%)")
                    logger.info(f"   Total Comparisons: {self.stats['comparisons']:,} | "
                              f"Duplicates: {self.stats['duplicates']:,} | "
                              f"Errors: {self.stats['errors']}")
                    logger.info(f"   Average Rate: {rate:.0f} comp/sec | "
                              f"Elapsed: {elapsed/60:.1f}min")
                    
                    # Estimate time remaining
                    if rate > 0 and completed < len(clusters):
                        clusters_remaining = len(clusters) - completed
                        # Rough estimate: assume similar comparison load per cluster
                        avg_comparisons_per_cluster = self.stats['comparisons'] / completed if completed > 0 else 0
                        estimated_comparisons_left = clusters_remaining * avg_comparisons_per_cluster
                        estimated_seconds_left = estimated_comparisons_left / rate
                        estimated_hours_left = estimated_seconds_left / 3600
                        
                        if estimated_hours_left >= 24:
                            logger.info(f"   Estimated time remaining: {estimated_hours_left/24:.1f} days")
                        elif estimated_hours_left >= 1:
                            logger.info(f"   Estimated time remaining: {estimated_hours_left:.1f} hours")
                        else:
                            logger.info(f"   Estimated time remaining: {estimated_seconds_left/60:.0f} minutes")
                    
                    logger.info("="*80 + "\n")
        
        # Flush any remaining records
        if self.output_file:
            logger.info("Flushing remaining records to file...")
            self.flush_file_buffer()
            self.flush_error_buffer()
            
            # Save final checkpoint and cleanup
            self.save_checkpoint(len(clusters), len(clusters), force=True)
            self.cleanup_checkpoint()
            
            logger.info(f"✅ All results written to: {self.output_file}")
            if self.stats['errors'] > 0:
                logger.info(f"⚠️  Errors written to: {self.error_file}")
        else:
            logger.info("Flushing remaining batch to Snowflake...")
            self.flush_duplicate_batch()
        
        # Final results
        with self.stats_lock:
            elapsed_time = time.time() - self.stats['start_time']
            results = {
                'duplicates': self.stats['duplicates'],
                'comparisons': self.stats['comparisons'],
                'skipped': self.stats['skipped'],
                'errors': self.stats['errors'],
                'clusters': len(clusters),
                'songs': len(songs),
                'time': elapsed_time,
                'rate': self.stats['comparisons'] / elapsed_time if elapsed_time > 0 else 0
            }
        
        logger.info(f"✅ Duplicate detection complete!")
        logger.info(f"📊 Found {results['duplicates']:,} duplicate pairs")
        logger.info(f"📊 Made {results['comparisons']:,} comparisons (skipped {results['skipped']:,} existing)")
        if results['errors'] > 0:
            logger.warning(f"⚠️  Encountered {results['errors']:,} errors (saved to {self.error_file if self.output_file else 'not tracked'})")
        logger.info(f"📊 Processed {results['clusters']:,} clusters in {elapsed_time/60:.1f} minutes")
        logger.info(f"📊 Rate: {results['rate']:.0f} comparisons/second")
        
        return results
    
    def get_stats(self) -> Dict:
        """Get statistics from the duplicates table"""
        stats_queries = {
            'total_duplicates': "SELECT COUNT(*) FROM AI_DATA.AUDIO_DETECTED_DUPLICATES",
            'by_type': """
                SELECT DUPLICATE_TYPE, COUNT(*) as count 
                FROM AI_DATA.AUDIO_DETECTED_DUPLICATES 
                GROUP BY DUPLICATE_TYPE 
                ORDER BY count DESC
            """,
            'by_similarity': """
                SELECT 
                    CASE 
                        WHEN SIMILARITY >= 0.95 THEN '0.95+'
                        WHEN SIMILARITY >= 0.90 THEN '0.90-0.95'
                        WHEN SIMILARITY >= 0.80 THEN '0.80-0.90'
                        WHEN SIMILARITY >= 0.60 THEN '0.60-0.80'
                        ELSE '<0.60'
                    END as similarity_range,
                    COUNT(*) as count
                FROM AI_DATA.AUDIO_DETECTED_DUPLICATES
                GROUP BY similarity_range
                ORDER BY MIN(SIMILARITY) DESC
            """,
            'cross_source': """
                SELECT COUNT(*) 
                FROM AI_DATA.AUDIO_DETECTED_DUPLICATES 
                WHERE SOURCE_1 != SOURCE_2
            """,
            'same_asset': """
                SELECT COUNT(*) 
                FROM AI_DATA.AUDIO_DETECTED_DUPLICATES 
                WHERE IS_SAME_ASSET = TRUE
            """,
            'different_asset': """
                SELECT COUNT(*) 
                FROM AI_DATA.AUDIO_DETECTED_DUPLICATES 
                WHERE IS_SAME_ASSET = FALSE
            """
        }
        
        stats = {}
        
        try:
            for stat_name, query in stats_queries.items():
                cursor = self.snowflake.execute_query(query)
                
                if stat_name in ['total_duplicates', 'cross_source', 'same_asset', 'different_asset']:
                    stats[stat_name] = cursor.fetchone()[0]
                else:
                    stats[stat_name] = cursor.fetchall()
                
                cursor.close()
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Failed to get stats: {e}")
            return {}
    
    def print_stats(self):
        """Print duplicate detection statistics"""
        stats = self.get_stats()
        
        if not stats:
            logger.error("❌ Could not retrieve statistics")
            return
        
        print("\n📊 Duplicate Detection Statistics")
        print("=" * 50)
        print(f"Total duplicate pairs found: {stats.get('total_duplicates', 0)}")
        print(f"Cross-source duplicates: {stats.get('cross_source', 0)}")
        print(f"Same asset duplicates: {stats.get('same_asset', 0)}")
        print(f"Different asset duplicates: {stats.get('different_asset', 0)}")
        
        print("\n📈 By Duplicate Type:")
        for type_name, count in stats.get('by_type', []):
            print(f"  {type_name}: {count}")
        
        print("\n📈 By Similarity Range:")
        for similarity_range, count in stats.get('by_similarity', []):
            print(f"  {similarity_range}: {count}")
    
    def load_and_upload_from_file(self, input_file: str) -> Dict:
        """
        Load duplicate results from file and upload to Snowflake in batches.
        
        Args:
            input_file: Path to JSON lines file with duplicate records
        
        Returns:
            Dict with upload statistics
        """
        logger.info(f"📥 Loading duplicates from: {input_file}")
        
        if not os.path.exists(input_file):
            logger.error(f"❌ File not found: {input_file}")
            return {'uploaded': 0, 'skipped': 0, 'errors': 0}
        
        # Ensure table exists
        self.ensure_duplicates_table_exists()
        
        start_time = time.time()
        uploaded = 0
        skipped = 0
        errors = 0
        
        batch = []
        
        try:
            with open(input_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        record = json.loads(line.strip())
                        
                        # Check if already exists (to avoid duplicates on re-upload)
                        if self.check_if_duplicate_exists(
                            record['asset_id_1'], record['file_key_1'],
                            record['asset_id_2'], record['file_key_2']
                        ):
                            skipped += 1
                            continue
                        
                        batch.append(record)
                        
                        # Flush batch if full
                        if len(batch) >= self.batch_size:
                            self._upload_batch(batch)
                            uploaded += len(batch)
                            batch.clear()
                            logger.info(f"📤 Uploaded {uploaded:,} records so far...")
                        
                    except json.JSONDecodeError as e:
                        logger.warning(f"⚠️  Skipping invalid JSON at line {line_num}: {e}")
                        errors += 1
                    except Exception as e:
                        logger.warning(f"⚠️  Error processing line {line_num}: {e}")
                        errors += 1
                
                # Flush remaining batch
                if batch:
                    self._upload_batch(batch)
                    uploaded += len(batch)
                    batch.clear()
            
            elapsed = time.time() - start_time
            
            logger.info(f"✅ Upload complete!")
            logger.info(f"   Uploaded: {uploaded:,} records")
            logger.info(f"   Skipped (already exist): {skipped:,}")
            logger.info(f"   Errors: {errors:,}")
            logger.info(f"   Time: {elapsed/60:.1f} minutes")
            
            return {
                'uploaded': uploaded,
                'skipped': skipped,
                'errors': errors,
                'time': elapsed
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to load and upload from file: {e}")
            raise
    
    def _upload_batch(self, batch: List[Dict]):
        """Upload a batch of records to Snowflake"""
        if not batch:
            return
        
        insert_sql = """
        INSERT INTO AI_DATA.AUDIO_DETECTED_DUPLICATES 
        (ASSET_ID_1, ASSET_ID_2, IS_SAME_ASSET, SIMILARITY, DUPLICATE_TYPE,
         FILE_KEY_1, FORMAT_1, SOURCE_1, DURATION_1,
         FILE_KEY_2, FORMAT_2, SOURCE_2, DURATION_2, DURATION_DIFF)
        VALUES (%(asset_id_1)s, %(asset_id_2)s, %(is_same_asset)s, %(similarity)s, %(duplicate_type)s,
                %(file_key_1)s, %(format_1)s, %(source_1)s, %(duration_1)s,
                %(file_key_2)s, %(format_2)s, %(source_2)s, %(duration_2)s, %(duration_diff)s)
        """
        
        try:
            cursor = self.snowflake.conn.cursor()
            cursor.executemany(insert_sql, batch)
            cursor.close()
        except Exception as e:
            logger.error(f"❌ Failed to upload batch: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'snowflake'):
            self.snowflake.close()

def main():
    parser = argparse.ArgumentParser(
        description='Smart Audio Duplicate Detector - UPGRADED VERSION',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Step 1: Detect duplicates (writes to auto-generated timestamped file)
  python duplicate_detector.py --mode cross-source --workers 8

  # Step 1 (with custom output file):
  python duplicate_detector.py --mode cross-source --output results/duplicates.jsonl

  # Analyze errors if any occurred:
  python duplicate_detector_retry_errors.py results/duplicates_errors.jsonl --analyze-only

  # Step 2: Load results from file and upload to Snowflake in batches
  python duplicate_detector.py --load-and-upload duplicate_results_cross-source_20250116_143022.jsonl

  # Show statistics from Snowflake
  python duplicate_detector.py --stats
        """
    )
    parser.add_argument('--mode', choices=['cross-source', 'same-source', 'all'], 
                       help='Comparison mode')
    parser.add_argument('--similarity-threshold', type=float, default=0.0,
                       help='Minimum similarity to store (default: 0.0 = store all for analysis)')
    parser.add_argument('--duration-tolerance', type=float, default=5.0,
                       help='Duration clustering tolerance in seconds (default: 5.0)')
    # Auto-detect optimal worker count (use 90% of cores, minimum 1)
    default_workers = max(1, int(cpu_count() * 0.9))
    parser.add_argument('--workers', type=int, default=default_workers,
                       help=f'Number of parallel PROCESSES (default: {default_workers}, auto-detected from {cpu_count()} cores)')
    parser.add_argument('--output', '--output-file', dest='output_file',
                       help='Output file for results (JSONL format). If specified, writes to file instead of Snowflake.')
    parser.add_argument('--load-and-upload', dest='load_file',
                       help='Load results from file and upload to Snowflake in batches')
    parser.add_argument('--stats', action='store_true',
                       help='Show statistics from Snowflake')
    parser.add_argument('--no-resume', action='store_true',
                       help='Start fresh even if checkpoint exists (default: resume from checkpoint)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.mode and not args.stats and not args.load_file:
        parser.error("Must specify --mode, --stats, or --load-and-upload")
    
    if args.mode and args.load_file:
        parser.error("Cannot specify both --mode and --load-and-upload")
    
    # Generate default output filename if mode is specified but no output file
    output_file = args.output_file
    if args.mode and not output_file:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"duplicate_results_{args.mode}_{timestamp}.jsonl"
        logger.info(f"📝 No output file specified, using: {output_file}")
    
    detector = DuplicateDetector(max_workers=args.workers, output_file=output_file)
    
    try:
        if args.stats:
            detector.print_stats()
        elif args.load_file:
            logger.info("="*80)
            logger.info(f"LOADING AND UPLOADING FROM FILE")
            logger.info("="*80)
            
            results = detector.load_and_upload_from_file(args.load_file)
            
            print("\n" + "="*80)
            print("📊 UPLOAD RESULTS")
            print("="*80)
            print(f"   Input file: {args.load_file}")
            print(f"   Records uploaded: {results['uploaded']:,}")
            print(f"   Skipped (already exist): {results['skipped']:,}")
            print(f"   Errors: {results['errors']:,}")
            print(f"   Upload time: {results['time']/60:.1f} minutes")
            print("="*80)
        else:
            logger.info("="*80)
            logger.info(f"DUPLICATE DETECTOR - {args.mode.upper()} MODE")
            logger.info("="*80)
            logger.info(f"🚀 MULTIPROCESSING MODE: {args.workers} parallel processes (detected {cpu_count()} cores)")
            logger.info(f"   Expected throughput: ~{args.workers * 6.5:.0f} comparisons/second")
            if output_file:
                logger.info(f"📝 Output mode: FILE ({output_file})")
            else:
                logger.info(f"📝 Output mode: SNOWFLAKE (direct write)")
            
            results = detector.detect_duplicates(
                mode=args.mode,
                similarity_threshold=args.similarity_threshold,
                duration_tolerance=args.duration_tolerance,
                resume=not args.no_resume
            )
            
            print("\n" + "="*80)
            print("📊 FINAL RESULTS")
            print("="*80)
            print(f"   Mode: {args.mode}")
            print(f"   Duplicate pairs found: {results['duplicates']:,}")
            print(f"   Comparisons made: {results['comparisons']:,}")
            print(f"   Skipped (already processed): {results['skipped']:,}")
            if results['errors'] > 0:
                print(f"   ⚠️  Errors encountered: {results['errors']:,}")
            print(f"   Duration clusters: {results['clusters']:,}")
            print(f"   Songs processed: {results['songs']:,}")
            print(f"   Processing time: {results['time']/60:.1f} minutes")
            print(f"   Comparison rate: {results['rate']:.0f} comparisons/second")
            
            if results['songs'] > 1:
                brute_force_comparisons = results['songs'] * (results['songs'] - 1) // 2
                efficiency = (1 - results['comparisons'] / brute_force_comparisons) * 100 if brute_force_comparisons > 0 else 0
                print(f"   Efficiency gain: {efficiency:.1f}% fewer comparisons than brute force")
            
            if output_file:
                print(f"\n   ✅ Results written to: {output_file}")
                if results['errors'] > 0:
                    error_file = output_file.replace('.jsonl', '_errors.jsonl')
                    print(f"   ⚠️  Errors written to: {error_file}")
                    print(f"   💡 To retry errors, you can process the error file separately")
                print(f"   💡 To upload to Snowflake, run:")
                print(f"      python duplicate_detector.py --load-and-upload {output_file}")
            
            print("="*80)
    
    finally:
        detector.close()

if __name__ == "__main__":
    main()
