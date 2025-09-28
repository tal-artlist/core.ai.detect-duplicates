#!/usr/bin/env python3
"""
Smart Audio Duplicate Detector

Loads fingerprints from Snowflake and uses duration-based clustering 
to efficiently find duplicates without N² comparisons.

Usage:
    python duplicate_detector.py --batch-size 1000
    python duplicate_detector.py --similarity-threshold 0.80
    python duplicate_detector.py --stats
"""

import os
import sys
import logging
import argparse
import tempfile
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
import subprocess
import time

# Auto-restart with correct environment if needed (same as fingerprint processor)
if 'DYLD_LIBRARY_PATH' not in os.environ or '/opt/homebrew/lib' not in os.environ.get('DYLD_LIBRARY_PATH', ''):
    env = os.environ.copy()
    env['DYLD_LIBRARY_PATH'] = '/opt/homebrew/lib:' + env.get('DYLD_LIBRARY_PATH', '')
    result = subprocess.run([sys.executable] + sys.argv, env=env)
    sys.exit(result.returncode)

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
    """Smart duplicate detector using duration-based clustering"""
    
    def __init__(self):
        """Initialize the duplicate detector"""
        self.snowflake = SnowflakeConnector()
        self.setup_chromaprint()
        
    def setup_chromaprint(self):
        """Set up Chromaprint library and environment (same as fingerprint processor)"""
        try:
            import ctypes
            chromaprint_lib = ctypes.CDLL('/opt/homebrew/lib/libchromaprint.dylib')
            
            fpcalc_path = "/opt/homebrew/bin/fpcalc"
            os.environ['FPCALC_COMMAND'] = fpcalc_path
            acoustid.FPCALC_COMMAND = fpcalc_path
            
            logger.info("✅ Chromaprint setup successful")
            return acoustid
        except Exception as e:
            logger.error(f"❌ Chromaprint setup failed: {e}")
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
    
    def load_fingerprints(self, batch_size: int = 1000) -> List[Dict]:
        """Load fingerprints from Snowflake"""
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
        ORDER BY DURATION
        LIMIT %(batch_size)s
        """
        
        try:
            cursor = self.snowflake.execute_query(query, {'batch_size': batch_size})
            
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
            logger.info(f"📊 Loaded {len(fingerprints)} fingerprints from Snowflake")
            return fingerprints
            
        except Exception as e:
            logger.error(f"❌ Failed to load fingerprints: {e}")
            raise
    
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
        """Compare two fingerprints and return similarity score"""
        try:
            # Convert string fingerprints to bytes (required by acoustid.compare_fingerprints)
            fp1_bytes = song1['fingerprint'].encode('utf-8')
            fp2_bytes = song2['fingerprint'].encode('utf-8')
            
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
    
    def find_duplicates_in_cluster(self, cluster: List[Dict], similarity_threshold: float = 0.80) -> List[Dict]:
        """Find duplicates within a single duration cluster"""
        duplicates = []
        
        for i in range(len(cluster)):
            for j in range(i + 1, len(cluster)):
                song1, song2 = cluster[i], cluster[j]
                
                # Skip if comparing the same file key (same exact file)
                if song1['file_key'] == song2['file_key']:
                    continue
                
                # Compare fingerprints
                similarity = self.compare_fingerprints(song1, song2)
                
                if similarity is not None and similarity >= similarity_threshold:
                    duplicate_type = self.classify_duplicate_type(song1, song2, similarity)
                    
                    duplicate = {
                        'song1': song1,
                        'song2': song2,
                        'similarity': similarity,
                        'type': duplicate_type
                    }
                    duplicates.append(duplicate)
                    
                    # Store in database
                    self.store_duplicate(song1, song2, similarity, duplicate_type)
                    
                    logger.info(f"🔍 Duplicate found: {song1['asset_id']} ({song1['format']}) <-> {song2['asset_id']} ({song2['format']}) "
                              f"({similarity:.3f}, {duplicate_type})")
        
        return duplicates
    
    def detect_duplicates(self, batch_size: int = 1000, similarity_threshold: float = 0.80, 
                         duration_tolerance: float = 5.0) -> Dict:
        """Main duplicate detection process"""
        logger.info(f"🚀 Starting duplicate detection (batch_size: {batch_size}, "
                   f"similarity_threshold: {similarity_threshold}, duration_tolerance: {duration_tolerance}s)")
        
        start_time = time.time()
        
        # Ensure tables exist
        self.ensure_duplicates_table_exists()
        
        # Load fingerprints
        songs = self.load_fingerprints(batch_size)
        
        if len(songs) < 2:
            logger.warning("⚠️  Not enough songs to compare")
            return {'duplicates': 0, 'comparisons': 0, 'clusters': 0}
        
        # Cluster by duration
        clusters = self.cluster_by_duration(songs, duration_tolerance)
        
        if not clusters:
            logger.warning("⚠️  No duration clusters found")
            return {'duplicates': 0, 'comparisons': 0, 'clusters': 0}
        
        # Find duplicates in each cluster
        total_duplicates = 0
        total_comparisons = 0
        
        for i, cluster in enumerate(clusters):
            logger.info(f"🔍 Processing cluster {i+1}/{len(clusters)} ({len(cluster)} songs, "
                       f"duration: {cluster[0]['duration']:.1f}s ± {duration_tolerance}s)")
            
            cluster_duplicates = self.find_duplicates_in_cluster(cluster, similarity_threshold)
            total_duplicates += len(cluster_duplicates)
            
            # Calculate comparisons for this cluster
            cluster_comparisons = len(cluster) * (len(cluster) - 1) // 2
            total_comparisons += cluster_comparisons
        
        elapsed_time = time.time() - start_time
        
        logger.info(f"✅ Duplicate detection complete!")
        logger.info(f"📊 Found {total_duplicates} duplicate pairs")
        logger.info(f"📊 Made {total_comparisons:,} comparisons (vs {len(songs)*(len(songs)-1)//2:,} brute force)")
        logger.info(f"📊 Processed {len(clusters)} clusters in {elapsed_time:.1f}s")
        
        return {
            'duplicates': total_duplicates,
            'comparisons': total_comparisons,
            'clusters': len(clusters),
            'songs': len(songs),
            'time': elapsed_time
        }
    
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
    parser = argparse.ArgumentParser(description='Smart Audio Duplicate Detector')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Number of songs to process (default: 1000)')
    parser.add_argument('--similarity-threshold', type=float, default=0.80,
                       help='Minimum similarity threshold (default: 0.80)')
    parser.add_argument('--duration-tolerance', type=float, default=5.0,
                       help='Duration clustering tolerance in seconds (default: 5.0)')
    parser.add_argument('--stats', action='store_true',
                       help='Show statistics only')
    
    args = parser.parse_args()
    
    detector = DuplicateDetector()
    
    try:
        if args.stats:
            detector.print_stats()
        else:
            results = detector.detect_duplicates(
                batch_size=args.batch_size,
                similarity_threshold=args.similarity_threshold,
                duration_tolerance=args.duration_tolerance
            )
            
            print(f"\n📊 Final Results:")
            print(f"   Duplicate pairs found: {results['duplicates']}")
            print(f"   Comparisons made: {results['comparisons']:,}")
            print(f"   Duration clusters: {results['clusters']}")
            print(f"   Songs processed: {results['songs']}")
            print(f"   Processing time: {results['time']:.1f}s")
            
            if results['songs'] > 1:
                brute_force_comparisons = results['songs'] * (results['songs'] - 1) // 2
                efficiency = (1 - results['comparisons'] / brute_force_comparisons) * 100
                print(f"   Efficiency gain: {efficiency:.1f}% fewer comparisons")
    
    finally:
        detector.close()

if __name__ == "__main__":
    main()
