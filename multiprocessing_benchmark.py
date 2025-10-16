#!/usr/bin/env python3
"""
Multiprocessing Benchmark Test

Tests multiprocessing performance on this system to see if we can achieve
high throughput before converting the main duplicate detector.
"""

import os
import sys
import logging  
import time
import statistics
import random
import shutil
from typing import List, Dict
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count

try:
    import acoustid
    from snowflake_utils import SnowflakeConnector
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_chromaprint_for_process():
    """Setup chromaprint for each process"""
    try:
        fpcalc_path = shutil.which('fpcalc')
        if not fpcalc_path:
            candidate_paths = ['/usr/bin/fpcalc', '/usr/local/bin/fpcalc']
            for path in candidate_paths:
                if os.path.exists(path):
                    fpcalc_path = path
                    break
        
        if fpcalc_path:
            os.environ['FPCALC_COMMAND'] = fpcalc_path
            acoustid.FPCALC_COMMAND = fpcalc_path
            return True
    except:
        pass
    return False

def process_comparison_worker(pair_data):
    """Single comparison worker for multiprocessing"""
    try:
        setup_chromaprint_for_process()
        
        fp1_data, fp2_data, pair_id = pair_data
        
        if fp1_data['file_key'] == fp2_data['file_key']:
            return {'pair_id': pair_id, 'skipped': True}
        
        fp1_bytes = fp1_data['fingerprint'].encode('utf-8')
        fp2_bytes = fp2_data['fingerprint'].encode('utf-8')
        
        start_time = time.perf_counter()
        
        similarity = acoustid.compare_fingerprints(
            (fp1_data['duration'], fp1_bytes),
            (fp2_data['duration'], fp2_bytes)
        )
        
        end_time = time.perf_counter()
        
        return {
            'pair_id': pair_id,
            'similarity': float(similarity),
            'time': end_time - start_time,
            'success': True
        }
        
    except Exception as e:
        return {'pair_id': pair_id, 'success': False, 'error': str(e)}

class MultiprocessingBenchmark:
    """Benchmark multiprocessing performance"""
    
    def __init__(self):
        self.snowflake = SnowflakeConnector()
        self.setup_chromaprint()
        
    def setup_chromaprint(self):
        """Set up Chromaprint library"""
        try:
            fpcalc_path = shutil.which('fpcalc')
            if not fpcalc_path:
                candidate_paths = ['/usr/bin/fpcalc', '/usr/local/bin/fpcalc']
                for path in candidate_paths:
                    if os.path.exists(path):
                        fpcalc_path = path
                        break
            
            if not fpcalc_path:
                raise RuntimeError("Could not find fpcalc binary")
            
            os.environ['FPCALC_COMMAND'] = fpcalc_path
            acoustid.FPCALC_COMMAND = fpcalc_path
            
            logger.info(f"✅ Chromaprint setup successful")
            return acoustid
        except Exception as e:
            logger.error(f"❌ Chromaprint setup failed: {e}")
            raise
    
    def load_test_fingerprints(self, limit: int = 500) -> List[Dict]:
        """Load test fingerprints"""
        query = f"""
        SELECT 
            ASSET_ID, FILE_KEY, FORMAT, DURATION, FINGERPRINT, SOURCE
        FROM AI_DATA.AUDIO_FINGERPRINT 
        WHERE PROCESSING_STATUS = 'SUCCESS'
            AND FINGERPRINT IS NOT NULL
            AND DURATION > 0
            AND DURATION BETWEEN 60 AND 300
        ORDER BY RANDOM()
        LIMIT {limit}
        """
        
        try:
            logger.info(f"📥 Loading {limit} test fingerprints...")
            cursor = self.snowflake.execute_query(query)
            
            fingerprints = []
            for row in cursor:
                fingerprints.append({
                    'asset_id': row[0],
                    'file_key': row[1],
                    'format': row[2],
                    'duration': float(row[3]),
                    'fingerprint': row[4],
                    'source': row[5]
                })
            
            cursor.close()
            logger.info(f"✅ Loaded {len(fingerprints)} test fingerprints")
            return fingerprints
            
        except Exception as e:
            logger.error(f"❌ Failed to load test fingerprints: {e}")
            raise
    
    def test_process_scaling(self, fingerprints: List[Dict], num_comparisons: int) -> Dict:
        """Test different numbers of processes"""
        logger.info(f"🔢 Testing process scaling with {num_comparisons} comparisons")
        
        # Test different process counts - focus on your 36 cores
        max_cpu = cpu_count()
        process_counts = [1, 2, 4, 8, 12, 16, 24, 32, max_cpu]
        process_counts = sorted(list(set([p for p in process_counts if p <= max_cpu and p >= 1])))
        
        results = {}
        
        for num_processes in process_counts:
            logger.info(f"⚙️  Testing {num_processes} processes...")
            
            # Generate pairs
            pairs = []
            for i in range(num_comparisons):
                fp1, fp2 = random.sample(fingerprints, 2)
                pairs.append((fp1, fp2, i))
            
            successful = 0
            comparison_times = []
            
            start_time = time.perf_counter()
            
            try:
                with ProcessPoolExecutor(max_workers=num_processes) as executor:
                    futures = [executor.submit(process_comparison_worker, pair) for pair in pairs]
                    
                    for future in as_completed(futures):
                        try:
                            result = future.result(timeout=30)
                            if result and result.get('success'):
                                comparison_times.append(result['time'])
                                successful += 1
                        except Exception as e:
                            logger.debug(f"Process failed: {e}")
            
            except Exception as e:
                logger.error(f"ProcessPoolExecutor failed: {e}")
            
            end_time = time.perf_counter()
            total_time = end_time - start_time
            
            throughput = successful / total_time if total_time > 0 else 0
            
            # Calculate efficiency vs single process
            baseline = results.get(1, {}).get('throughput', throughput / num_processes)
            efficiency = (throughput / (baseline * num_processes)) * 100 if baseline > 0 else 0
            
            results[num_processes] = {
                'processes': num_processes,
                'throughput': throughput,
                'total_time': total_time,
                'successful': successful,
                'efficiency': efficiency,
                'avg_comp_time': statistics.mean(comparison_times) if comparison_times else 0
            }
            
            logger.info(f"   ✅ {num_processes} processes: {throughput:.1f} comp/sec ({efficiency:.0f}% efficiency)")
        
        return results
    
    def run_benchmark(self):
        """Run comprehensive benchmark"""
        fingerprints = self.load_test_fingerprints(limit=500)
        
        if len(fingerprints) < 10:
            raise ValueError("Need at least 10 fingerprints for testing")
        
        print("\n" + "="*80)
        print("🚀 MULTIPROCESSING BENCHMARK - 36 CORE SYSTEM")
        print("="*80)
        print(f"System CPU cores: {cpu_count()}")
        print(f"Test fingerprints: {len(fingerprints)}")
        
        num_comparisons = 1000
        
        print(f"\n📊 PROCESS SCALING TEST ({num_comparisons} comparisons)")
        print("="*80)
        
        scaling_results = self.test_process_scaling(fingerprints, num_comparisons)
        
        print(f"\n{'Processes':<12} {'Throughput':<15} {'Speedup':<10} {'Efficiency':<12} {'Avg Time':<10}")
        print("-" * 70)
        
        baseline_throughput = scaling_results[1]['throughput']
        best_throughput = 0
        best_processes = 1
        
        for processes in sorted(scaling_results.keys()):
            result = scaling_results[processes]
            speedup = result['throughput'] / baseline_throughput
            throughput_str = f"{result['throughput']:.1f} c/s"
            speedup_str = f"{speedup:.1f}x"
            efficiency_str = f"{result['efficiency']:.0f}%"
            comp_time_str = f"{result['avg_comp_time']*1000:.1f}ms"
            
            marker = " 🎯" if result['throughput'] > best_throughput else ""
            print(f"{processes:<12} {throughput_str:<15} {speedup_str:<10} {efficiency_str:<12} {comp_time_str:<10}{marker}")
            
            if result['throughput'] > best_throughput:
                best_throughput = result['throughput']
                best_processes = processes
        
        print(f"\n" + "="*80)
        print("🎯 BENCHMARK RESULTS")
        print("="*80)
        print(f"✅ Peak Performance:")
        print(f"   • Optimal processes: {best_processes}")
        print(f"   • Peak throughput: {best_throughput:.1f} comparisons/sec")
        print(f"   • Maximum speedup: {best_throughput/baseline_throughput:.1f}x vs single process")
        
        # Calculate processing times for full dataset
        print(f"\n🚀 ESTIMATED FULL PROCESSING TIMES:")
        print(f"   (274,234 fingerprints, cross-source comparisons)")
        print("-" * 80)
        
        # Cross-source comparisons
        artlist = 137117
        motionarray = 137117
        
        # With different duration tolerances
        tolerances = {
            '1.0s': 0.10,  # 10% of pairs after clustering
            '5.0s': 0.02,  # 2% of pairs after clustering
            '10.0s': 0.01  # 1% of pairs after clustering
        }
        
        for tol_name, reduction in tolerances.items():
            total_pairs = artlist * motionarray
            realistic_comparisons = int(total_pairs * reduction)
            days = realistic_comparisons / best_throughput / 86400
            hours = (days - int(days)) * 24
            
            print(f"   ±{tol_name} tolerance: {days:.1f} days ({realistic_comparisons:,} comparisons)")
        
        print(f"\n" + "="*80)
        
        if best_throughput > 100:
            print("🎉 EXCELLENT! Your system can achieve 100+ comparisons/sec!")
            print("   This makes comprehensive duplicate detection practical.")
        elif best_throughput > 50:
            print("✅ GOOD! Your system shows solid multiprocessing performance.")
            print("   Cross-source duplicate detection is feasible.")
        else:
            print("⚠️  Performance is limited. May need further optimization.")
        
        print("="*80)
        
        return scaling_results
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'snowflake'):
            self.snowflake.close()

def main():
    """Run the benchmark"""
    benchmark = MultiprocessingBenchmark()
    
    try:
        benchmark.run_benchmark()
    except Exception as e:
        logger.error(f"❌ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        benchmark.close()

if __name__ == "__main__":
    main()

