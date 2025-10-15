#!/usr/bin/env python3
"""
Smart Audio Duplicate Detector - UPGRADED VERSION

Loads ALL fingerprints from Snowflake and uses duration-based clustering 
with parallel processing to efficiently find duplicates.

Prioritizes cross-source comparisons (artlist ↔ motionarray).

Usage:
    # Cross-source comparisons (priority)
    python duplicate_detector.py --mode cross-source --workers 8
    
    # Same-source comparisons
    python duplicate_detector.py --mode same-source --workers 8
    
    # All comparisons
    python duplicate_detector.py --mode all --workers 8
    
    # Stats only
    python duplicate_detector.py --stats
"""

import os
import sys
import logging
import argparse
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
import subprocess
import time
import platform
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import ctypes

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

class DuplicateDetector:
    """Smart duplicate detector using duration-based clustering with parallel processing"""
    
    def __init__(self, max_workers: int = 4):
        """Initialize the duplicate detector"""
        self.snowflake = SnowflakeConnector()
        self.setup_chromaprint()
        self.max_workers = max_workers
        self.stats_lock = Lock()
        self.comparison_lock = Lock()  # Lock for thread-safe fingerprint comparison
        self.stats = {
            'comparisons': 0,
            'duplicates': 0,
            'skipped': 0,
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
    
    def store_duplicate(self, song1: Dict, song2: Dict, similarity: float, duplicate_type: str):
        """Store duplicate pair in Snowflake"""
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
            self.snowflake.execute_query(insert_sql, {
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
            })
            logger.debug(f"✅ Stored duplicate: {song1['asset_id']} <-> {song2['asset_id']} ({similarity:.3f})")
        except Exception as e:
            logger.error(f"❌ Failed to store duplicate: {e}")
    
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
            
            # Update comparison count
            with self.stats_lock:
                self.stats['comparisons'] += 1
            
            if similarity is not None and similarity >= similarity_threshold:
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
            logger.error(f"❌ Failed to compare pair: {e}")
            return None
    
    def find_duplicates_in_cluster_parallel(self, cluster: List[Dict], mode: str, 
                                           similarity_threshold: float = 0.0) -> int:
        """
        Find duplicates within a single duration cluster using parallel processing.
        
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
        
        # Process pairs in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.compare_and_store_pair, pair, similarity_threshold) 
                      for pair in pairs]
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result is not None:
                        duplicates_found += 1
                except Exception as e:
                    logger.error(f"❌ Pair comparison failed: {e}")
        
        return duplicates_found
    
    def detect_duplicates(self, mode: str = 'cross-source', 
                         similarity_threshold: float = 0.0,
                         duration_tolerance: float = 5.0) -> Dict:
        """
        Main duplicate detection process with parallel processing.
        
        Args:
            mode: 'cross-source' (artlist ↔ motionarray), 'same-source', or 'all'
            similarity_threshold: Minimum similarity to store (0.0 = store all for later analysis)
            duration_tolerance: Duration clustering tolerance in seconds
        
        Returns:
            Dict with processing statistics
        """
        logger.info(f"🚀 Starting duplicate detection")
        logger.info(f"   Mode: {mode}")
        logger.info(f"   Similarity threshold: {similarity_threshold} (0.0 = store all)")
        logger.info(f"   Duration tolerance: ±{duration_tolerance}s")
        logger.info(f"   Parallel workers: {self.max_workers}")
        
        start_time = time.time()
        
        # Initialize stats
        with self.stats_lock:
            self.stats = {
                'comparisons': 0,
                'duplicates': 0,
                'skipped': 0,
                'start_time': start_time
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
        
        # Count expected comparisons
        total_expected_pairs = 0
        for cluster in clusters:
            pairs = self.filter_cluster_by_mode(cluster, mode)
            total_expected_pairs += len(pairs)
        
        logger.info(f"📊 Will process {len(clusters):,} clusters with {total_expected_pairs:,} comparisons")
        
        # Process each cluster
        for i, cluster in enumerate(clusters, 1):
            cluster_pairs = self.filter_cluster_by_mode(cluster, mode)
            
            if not cluster_pairs:
                continue
            
            logger.info(f"🔍 Cluster {i}/{len(clusters)} | Duration: {cluster[0]['duration']:.1f}s | "
                       f"Songs: {len(cluster)} | Pairs to compare: {len(cluster_pairs):,}")
            
            # Process cluster in parallel
            self.find_duplicates_in_cluster_parallel(cluster, mode, similarity_threshold)
            
            # Progress update
            if i % 100 == 0 or i == len(clusters):
                with self.stats_lock:
                    elapsed = time.time() - self.stats['start_time']
                    rate = self.stats['comparisons'] / elapsed if elapsed > 0 else 0
                    
                    logger.info(f"📈 Progress: {i}/{len(clusters)} clusters | "
                              f"Comparisons: {self.stats['comparisons']:,}/{total_expected_pairs:,} | "
                              f"Duplicates: {self.stats['duplicates']:,} | "
                              f"Rate: {rate:.0f} comp/sec | "
                              f"Elapsed: {elapsed/60:.1f}min")
        
        # Final results
        with self.stats_lock:
            elapsed_time = time.time() - self.stats['start_time']
            results = {
                'duplicates': self.stats['duplicates'],
                'comparisons': self.stats['comparisons'],
                'skipped': self.stats['skipped'],
                'clusters': len(clusters),
                'songs': len(songs),
                'time': elapsed_time,
                'rate': self.stats['comparisons'] / elapsed_time if elapsed_time > 0 else 0
            }
        
        logger.info(f"✅ Duplicate detection complete!")
        logger.info(f"📊 Found {results['duplicates']:,} duplicate pairs")
        logger.info(f"📊 Made {results['comparisons']:,} comparisons (skipped {results['skipped']:,} existing)")
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
  # Cross-source comparisons (PRIORITY - artlist ↔ motionarray)
  python duplicate_detector.py --mode cross-source --workers 8

  # Same-source comparisons (artlist ↔ artlist, motionarray ↔ motionarray)
  python duplicate_detector.py --mode same-source --workers 8

  # All comparisons
  python duplicate_detector.py --mode all --workers 8

  # Show statistics
  python duplicate_detector.py --stats
        """
    )
    parser.add_argument('--mode', choices=['cross-source', 'same-source', 'all'], 
                       default='cross-source',
                       help='Comparison mode (default: cross-source)')
    parser.add_argument('--similarity-threshold', type=float, default=0.0,
                       help='Minimum similarity to store (default: 0.0 = store all for analysis)')
    parser.add_argument('--duration-tolerance', type=float, default=5.0,
                       help='Duration clustering tolerance in seconds (default: 5.0)')
    parser.add_argument('--workers', type=int, default=4,
                       help='Number of parallel workers (default: 4)')
    parser.add_argument('--stats', action='store_true',
                       help='Show statistics only')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.stats and not args.mode:
        parser.error("Must specify --mode or --stats")
    
    detector = DuplicateDetector(max_workers=args.workers)
    
    try:
        if args.stats:
            detector.print_stats()
        else:
            logger.info("="*80)
            logger.info(f"DUPLICATE DETECTOR - {args.mode.upper()} MODE")
            logger.info("="*80)
            
            results = detector.detect_duplicates(
                mode=args.mode,
                similarity_threshold=args.similarity_threshold,
                duration_tolerance=args.duration_tolerance
            )
            
            print("\n" + "="*80)
            print("📊 FINAL RESULTS")
            print("="*80)
            print(f"   Mode: {args.mode}")
            print(f"   Duplicate pairs found: {results['duplicates']:,}")
            print(f"   Comparisons made: {results['comparisons']:,}")
            print(f"   Skipped (already processed): {results['skipped']:,}")
            print(f"   Duration clusters: {results['clusters']:,}")
            print(f"   Songs processed: {results['songs']:,}")
            print(f"   Processing time: {results['time']/60:.1f} minutes")
            print(f"   Comparison rate: {results['rate']:.0f} comparisons/second")
            
            if results['songs'] > 1:
                brute_force_comparisons = results['songs'] * (results['songs'] - 1) // 2
                efficiency = (1 - results['comparisons'] / brute_force_comparisons) * 100 if brute_force_comparisons > 0 else 0
                print(f"   Efficiency gain: {efficiency:.1f}% fewer comparisons than brute force")
            print("="*80)
    
    finally:
        detector.close()

if __name__ == "__main__":
    main()
