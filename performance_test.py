#!/usr/bin/env python3
"""
Duplicate Detector - Performance Testing Tool

This script benchmarks the duplicate detection system to find optimal settings:
- Tests different worker counts (1, 2, 4, 8, 12, 16, 24)
- Measures fingerprint comparison throughput
- Estimates total processing time for full dataset
- Tests memory usage and system resource utilization
- Provides recommendations for optimal configuration

Usage:
    # Quick test with small sample
    python performance_test.py --sample-size 1000

    # Full benchmark test
    python performance_test.py --sample-size 5000 --test-all-workers

    # Test specific worker count
    python performance_test.py --workers 8 --sample-size 2000

    # Include memory profiling
    python performance_test.py --sample-size 1000 --profile-memory
"""

import os
import sys
import time
import psutil
import argparse
import logging
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import json
from datetime import datetime, timedelta
import statistics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from duplicate_detector import DuplicateDetector
    from snowflake_utils import SnowflakeConnector
except ImportError as e:
    logger.error(f"❌ Import error: {e}")
    logger.error("Make sure you're in the correct conda environment: conda activate audio-duplicate-detection")
    sys.exit(1)

class PerformanceTester:
    """Performance testing suite for duplicate detection system"""
    
    def __init__(self, sample_size: int = 1000, profile_memory: bool = False):
        self.sample_size = sample_size
        self.profile_memory = profile_memory
        self.snowflake = SnowflakeConnector()
        self.results = []
        self.system_info = self.get_system_info()
        
    def get_system_info(self) -> Dict:
        """Get system information for context"""
        return {
            'cpu_count': psutil.cpu_count(),
            'cpu_count_logical': psutil.cpu_count(logical=True),
            'memory_total_gb': psutil.virtual_memory().total / (1024**3),
            'memory_available_gb': psutil.virtual_memory().available / (1024**3),
            'platform': sys.platform,
            'python_version': sys.version.split()[0]
        }
    
    def load_sample_data(self) -> List[Dict]:
        """Load a sample of fingerprint data for testing"""
        logger.info(f"📥 Loading {self.sample_size:,} fingerprint samples...")
        
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
        ORDER BY RANDOM()
        LIMIT %(limit)s
        """
        
        try:
            start_time = time.time()
            cursor = self.snowflake.execute_query(query, {'limit': self.sample_size})
            
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
            
            logger.info(f"✅ Loaded {len(fingerprints):,} samples in {load_time:.1f}s")
            return fingerprints
            
        except Exception as e:
            logger.error(f"❌ Failed to load sample data: {e}")
            raise
    
    def get_total_dataset_size(self) -> int:
        """Get the total number of fingerprints in the database"""
        query = """
        SELECT COUNT(*) 
        FROM AI_DATA.AUDIO_FINGERPRINT 
        WHERE PROCESSING_STATUS = 'SUCCESS'
            AND FINGERPRINT IS NOT NULL
            AND DURATION > 0
        """
        
        try:
            cursor = self.snowflake.execute_query(query)
            total_count = cursor.fetchone()[0]
            cursor.close()
            return total_count
        except Exception as e:
            logger.warning(f"⚠️  Could not get total dataset size: {e}")
            return 0
    
    def create_test_pairs(self, songs: List[Dict], max_pairs: int = 1000) -> List[Tuple[Dict, Dict]]:
        """Create pairs for testing (cross-source prioritized)"""
        pairs = []
        
        # Separate by source
        by_source = {}
        for song in songs:
            source = song['source']
            if source not in by_source:
                by_source[source] = []
            by_source[source].append(song)
        
        sources = list(by_source.keys())
        
        # Create cross-source pairs first (higher priority)
        for i, source1 in enumerate(sources):
            for source2 in sources[i+1:]:
                songs1 = by_source[source1]
                songs2 = by_source[source2]
                
                for song1 in songs1[:50]:  # Limit per source pair
                    for song2 in songs2[:50]:
                        if len(pairs) >= max_pairs:
                            break
                        pairs.append((song1, song2))
                    if len(pairs) >= max_pairs:
                        break
                if len(pairs) >= max_pairs:
                    break
            if len(pairs) >= max_pairs:
                break
        
        # Fill remaining with same-source pairs if needed
        if len(pairs) < max_pairs:
            for source, songs_in_source in by_source.items():
                for i in range(len(songs_in_source)):
                    for j in range(i+1, len(songs_in_source)):
                        if len(pairs) >= max_pairs:
                            break
                        pairs.append((songs_in_source[i], songs_in_source[j]))
                    if len(pairs) >= max_pairs:
                        break
                if len(pairs) >= max_pairs:
                    break
        
        logger.info(f"🔍 Created {len(pairs):,} test pairs")
        return pairs[:max_pairs]
    
    def benchmark_workers(self, pairs: List[Tuple[Dict, Dict]], worker_count: int) -> Dict:
        """Benchmark performance with specific worker count"""
        logger.info(f"🧪 Testing {worker_count} workers with {len(pairs):,} pairs...")
        
        # Initialize detector (no output file for testing)
        detector = DuplicateDetector(max_workers=worker_count, output_file=None)
        
        # Memory tracking
        process = psutil.Process()
        initial_memory = process.memory_info().rss / (1024**2)  # MB
        
        # Performance tracking
        stats = {
            'comparisons': 0,
            'successes': 0,
            'failures': 0,
            'duplicates_found': 0
        }
        stats_lock = Lock()
        
        def test_comparison(pair):
            """Test a single comparison"""
            song1, song2 = pair
            try:
                result = detector.compare_and_store_pair(pair, similarity_threshold=0.0)
                
                with stats_lock:
                    stats['comparisons'] += 1
                    if result is not None:
                        stats['successes'] += 1
                        if result.get('similarity', 0) > 0.6:  # Consider >0.6 as duplicate
                            stats['duplicates_found'] += 1
                    else:
                        stats['failures'] += 1
                
                return True
            except Exception as e:
                with stats_lock:
                    stats['failures'] += 1
                return False
        
        # Run benchmark
        start_time = time.time()
        start_cpu_times = psutil.cpu_times()
        
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(test_comparison, pair) for pair in pairs]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.debug(f"Comparison failed: {e}")
        
        end_time = time.time()
        end_cpu_times = psutil.cpu_times()
        
        # Calculate metrics
        elapsed_time = end_time - start_time
        final_memory = process.memory_info().rss / (1024**2)  # MB
        memory_used = final_memory - initial_memory
        
        cpu_user_time = end_cpu_times.user - start_cpu_times.user
        cpu_system_time = end_cpu_times.system - start_cpu_times.system
        
        throughput = stats['comparisons'] / elapsed_time if elapsed_time > 0 else 0
        success_rate = (stats['successes'] / stats['comparisons'] * 100) if stats['comparisons'] > 0 else 0
        
        result = {
            'worker_count': worker_count,
            'pairs_tested': len(pairs),
            'elapsed_time': elapsed_time,
            'throughput_per_sec': throughput,
            'success_rate_pct': success_rate,
            'duplicates_found': stats['duplicates_found'],
            'memory_used_mb': memory_used,
            'cpu_user_time': cpu_user_time,
            'cpu_system_time': cpu_system_time,
            'cpu_efficiency': (cpu_user_time + cpu_system_time) / elapsed_time if elapsed_time > 0 else 0
        }
        
        logger.info(f"✅ {worker_count} workers: {throughput:.0f} comp/sec, "
                   f"{success_rate:.1f}% success, {memory_used:.0f}MB memory")
        
        # Cleanup
        detector.close()
        
        return result
    
    def estimate_full_processing_time(self, throughput_per_sec: float, total_songs: int, 
                                    mode: str = 'cross-source') -> Dict:
        """Estimate processing time for full dataset"""
        if total_songs == 0:
            return {'error': 'Could not determine dataset size'}
        
        # Estimate total comparisons based on mode
        if mode == 'cross-source':
            # Assume roughly 50/50 split between sources
            source1_count = total_songs // 2
            source2_count = total_songs - source1_count
            estimated_comparisons = source1_count * source2_count
        elif mode == 'all':
            # All pairs
            estimated_comparisons = total_songs * (total_songs - 1) // 2
        else:  # same-source
            # Assume equal distribution across sources
            avg_per_source = total_songs // 2  # Assume 2 main sources
            estimated_comparisons = 2 * (avg_per_source * (avg_per_source - 1) // 2)
        
        if throughput_per_sec <= 0:
            return {'error': 'Invalid throughput'}
        
        estimated_seconds = estimated_comparisons / throughput_per_sec
        estimated_hours = estimated_seconds / 3600
        estimated_days = estimated_hours / 24
        
        return {
            'total_songs': total_songs,
            'estimated_comparisons': estimated_comparisons,
            'throughput_per_sec': throughput_per_sec,
            'estimated_seconds': estimated_seconds,
            'estimated_hours': estimated_hours,
            'estimated_days': estimated_days,
            'estimated_duration_str': str(timedelta(seconds=int(estimated_seconds)))
        }
    
    def run_performance_test(self, worker_counts: List[int]) -> Dict:
        """Run comprehensive performance test"""
        logger.info("🚀 Starting performance benchmark...")
        logger.info(f"System: {self.system_info['cpu_count']} cores, "
                   f"{self.system_info['memory_total_gb']:.1f}GB RAM")
        
        # Load sample data
        songs = self.load_sample_data()
        if len(songs) < 10:
            raise ValueError("Not enough sample data loaded")
        
        # Create test pairs
        test_pairs = self.create_test_pairs(songs, max_pairs=min(1000, len(songs) * 2))
        
        # Get total dataset size for projections
        total_songs = self.get_total_dataset_size()
        
        # Run benchmarks
        results = []
        for worker_count in worker_counts:
            try:
                result = self.benchmark_workers(test_pairs, worker_count)
                results.append(result)
                
                # Add time estimates
                if result['throughput_per_sec'] > 0:
                    estimates = self.estimate_full_processing_time(
                        result['throughput_per_sec'], total_songs, 'cross-source'
                    )
                    result['time_estimates'] = estimates
                
            except Exception as e:
                logger.error(f"❌ Failed to benchmark {worker_count} workers: {e}")
                continue
        
        # Find optimal configuration
        optimal = self.find_optimal_config(results)
        
        return {
            'system_info': self.system_info,
            'test_config': {
                'sample_size': self.sample_size,
                'test_pairs': len(test_pairs),
                'total_dataset_size': total_songs
            },
            'results': results,
            'optimal_config': optimal,
            'timestamp': datetime.now().isoformat()
        }
    
    def find_optimal_config(self, results: List[Dict]) -> Dict:
        """Find optimal worker configuration"""
        if not results:
            return {}
        
        # Sort by throughput
        by_throughput = sorted(results, key=lambda x: x['throughput_per_sec'], reverse=True)
        
        # Find best efficiency (throughput per core)
        for result in results:
            result['efficiency'] = result['throughput_per_sec'] / result['worker_count']
        
        by_efficiency = sorted(results, key=lambda x: x['efficiency'], reverse=True)
        
        # Find sweet spot (good throughput with reasonable memory usage)
        reasonable_memory = [r for r in results if r['memory_used_mb'] < 2000]  # < 2GB
        if reasonable_memory:
            by_sweet_spot = sorted(reasonable_memory, key=lambda x: x['throughput_per_sec'], reverse=True)
            sweet_spot = by_sweet_spot[0]
        else:
            sweet_spot = by_throughput[0]
        
        return {
            'fastest': by_throughput[0],
            'most_efficient': by_efficiency[0],
            'recommended': sweet_spot,
            'reasoning': f"Recommended {sweet_spot['worker_count']} workers for best balance of speed and resource usage"
        }
    
    def print_results(self, benchmark_results: Dict):
        """Print formatted benchmark results"""
        print("\n" + "="*80)
        print("🚀 DUPLICATE DETECTOR PERFORMANCE BENCHMARK")
        print("="*80)
        
        # System info
        sys_info = benchmark_results['system_info']
        print(f"System: {sys_info['cpu_count']} cores ({sys_info['cpu_count_logical']} logical), "
              f"{sys_info['memory_total_gb']:.1f}GB RAM")
        
        # Test config
        test_config = benchmark_results['test_config']
        print(f"Test: {test_config['sample_size']:,} samples, {test_config['test_pairs']:,} comparisons")
        print(f"Dataset: {test_config['total_dataset_size']:,} total fingerprints")
        
        print("\n📊 PERFORMANCE RESULTS:")
        print("-" * 80)
        print(f"{'Workers':<8} {'Throughput':<12} {'Success%':<9} {'Memory':<10} {'CPU Eff':<8} {'Est. Time':<12}")
        print("-" * 80)
        
        for result in benchmark_results['results']:
            throughput = f"{result['throughput_per_sec']:.0f}/sec"
            success = f"{result['success_rate_pct']:.1f}%"
            memory = f"{result['memory_used_mb']:.0f}MB"
            cpu_eff = f"{result['cpu_efficiency']:.1f}x"
            
            est_time = "N/A"
            if 'time_estimates' in result and 'estimated_duration_str' in result['time_estimates']:
                est_time = result['time_estimates']['estimated_duration_str']
            
            print(f"{result['worker_count']:<8} {throughput:<12} {success:<9} {memory:<10} {cpu_eff:<8} {est_time:<12}")
        
        # Optimal configuration
        if 'optimal_config' in benchmark_results:
            optimal = benchmark_results['optimal_config']
            print("\n🎯 RECOMMENDATIONS:")
            print("-" * 80)
            
            if 'fastest' in optimal:
                fastest = optimal['fastest']
                print(f"🏃 Fastest: {fastest['worker_count']} workers ({fastest['throughput_per_sec']:.0f} comp/sec)")
            
            if 'most_efficient' in optimal:
                efficient = optimal['most_efficient']
                print(f"⚡ Most Efficient: {efficient['worker_count']} workers ({efficient['efficiency']:.1f} comp/sec/core)")
            
            if 'recommended' in optimal:
                recommended = optimal['recommended']
                print(f"✅ Recommended: {recommended['worker_count']} workers")
                print(f"   Reason: {optimal.get('reasoning', 'Best overall balance')}")
                
                if 'time_estimates' in recommended:
                    est = recommended['time_estimates']
                    if 'estimated_duration_str' in est:
                        print(f"   Estimated full processing time: {est['estimated_duration_str']}")
                        print(f"   ({est['estimated_comparisons']:,} total comparisons)")
        
        print("="*80)
    
    def save_results(self, results: Dict, filename: str = None):
        """Save results to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"performance_benchmark_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"📝 Results saved to: {filename}")
        except Exception as e:
            logger.error(f"❌ Failed to save results: {e}")
    
    def close(self):
        """Cleanup resources"""
        if hasattr(self, 'snowflake'):
            self.snowflake.close()

def main():
    parser = argparse.ArgumentParser(
        description='Performance Testing Tool for Duplicate Detector',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--sample-size', type=int, default=1000,
                       help='Number of fingerprint samples to test with (default: 1000)')
    parser.add_argument('--workers', type=int,
                       help='Test specific worker count (default: test multiple)')
    parser.add_argument('--test-all-workers', action='store_true',
                       help='Test all worker counts from 1 to CPU count')
    parser.add_argument('--profile-memory', action='store_true',
                       help='Include detailed memory profiling')
    parser.add_argument('--save-results', action='store_true',
                       help='Save results to JSON file')
    
    args = parser.parse_args()
    
    # Determine worker counts to test
    if args.workers:
        worker_counts = [args.workers]
    elif args.test_all_workers:
        max_workers = psutil.cpu_count()
        worker_counts = [1, 2, 4, 8, 12, 16, 24, 32]
        worker_counts = [w for w in worker_counts if w <= max_workers * 2]
    else:
        # Default: test common configurations
        max_workers = psutil.cpu_count()
        worker_counts = [1, 4, max_workers, max_workers * 2]
        worker_counts = list(set([w for w in worker_counts if w <= 32]))  # Cap at 32
    
    logger.info(f"🧪 Testing worker counts: {worker_counts}")
    
    # Run performance test
    tester = PerformanceTester(
        sample_size=args.sample_size,
        profile_memory=args.profile_memory
    )
    
    try:
        results = tester.run_performance_test(worker_counts)
        tester.print_results(results)
        
        if args.save_results:
            tester.save_results(results)
        
    finally:
        tester.close()

if __name__ == "__main__":
    main()
