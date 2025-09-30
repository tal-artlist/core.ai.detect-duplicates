#!/usr/bin/env python3
"""
Worker Benchmark Script

This script helps determine the optimal number of workers for your specific machine
by testing different worker counts on a small sample of assets.

Usage:
    python benchmark_workers.py --source artlist --sample-size 20
    python benchmark_workers.py --source motionarray --sample-size 15
"""

import os
import sys
import subprocess
import argparse
import time
import logging
from typing import List, Dict
import multiprocessing
import psutil

# Auto-restart with correct environment if needed
if 'DYLD_LIBRARY_PATH' not in os.environ or '/opt/homebrew/lib' not in os.environ.get('DYLD_LIBRARY_PATH', ''):
    env = os.environ.copy()
    env['DYLD_LIBRARY_PATH'] = '/opt/homebrew/lib:' + env.get('DYLD_LIBRARY_PATH', '')
    result = subprocess.run([sys.executable] + sys.argv, env=env)
    sys.exit(result.returncode)

from audio_fingerprint_processor import AudioFingerprintProcessor

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Reduce noise during benchmarking
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WorkerBenchmark:
    """Benchmark different worker counts to find optimal performance"""
    
    def __init__(self):
        self.system_info = self.get_system_info()
        
    def get_system_info(self) -> Dict:
        """Get system information for benchmark context"""
        return {
            'cpu_count': multiprocessing.cpu_count(),
            'cpu_count_physical': psutil.cpu_count(logical=False),
            'memory_gb': round(psutil.virtual_memory().total / (1024**3), 1),
            'cpu_freq_max': psutil.cpu_freq().max if psutil.cpu_freq() else 'Unknown',
            'cpu_usage_percent': psutil.cpu_percent(interval=1)
        }
    
    def print_system_info(self):
        """Print system information"""
        print("🖥️  System Information:")
        print(f"   CPU Cores (Logical): {self.system_info['cpu_count']}")
        print(f"   CPU Cores (Physical): {self.system_info['cpu_count_physical']}")
        print(f"   Memory: {self.system_info['memory_gb']} GB")
        print(f"   CPU Max Frequency: {self.system_info['cpu_freq_max']} MHz")
        print(f"   Current CPU Usage: {self.system_info['cpu_usage_percent']}%")
        print()
    
    def get_sample_assets(self, source: str, sample_size: int) -> List[Dict]:
        """Get a small sample of assets for benchmarking"""
        processor = AudioFingerprintProcessor(max_workers=1)
        
        try:
            # Get all assets but limit to sample size
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
                    AND af.asset_id IS NULL
                ORDER BY RANDOM()
                LIMIT %(sample_size)s
                """
            else:  # motionarray
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
                    AND af.asset_id IS NULL
                ORDER BY RANDOM()
                LIMIT %(sample_size)s
                """
            
            cursor = processor.snowflake.execute_query(query, {'sample_size': sample_size})
            results = cursor.fetchall()
            
            if results:
                columns = [desc[0] for desc in cursor.description]
                assets = [dict(zip(columns, row)) for row in results]
                print(f"📊 Selected {len(assets)} random {source} assets for benchmarking")
                return assets
            else:
                print(f"⚠️  No unprocessed {source} assets found for benchmarking")
                return []
                
        except Exception as e:
            print(f"❌ Failed to get sample assets: {e}")
            return []
    
    def benchmark_worker_count(self, assets: List[Dict], worker_count: int) -> Dict:
        """Benchmark processing with specific worker count"""
        print(f"🧪 Testing {worker_count} workers...")
        
        # Create processor with specific worker count
        processor = AudioFingerprintProcessor(max_workers=worker_count)
        
        # Record system state before
        cpu_before = psutil.cpu_percent()
        memory_before = psutil.virtual_memory().percent
        
        # Process assets and measure time
        start_time = time.time()
        results = processor.process_assets_parallel(assets)
        end_time = time.time()
        
        # Record system state after
        cpu_after = psutil.cpu_percent()
        memory_after = psutil.virtual_memory().percent
        
        total_time = end_time - start_time
        
        benchmark_result = {
            'workers': worker_count,
            'total_time': total_time,
            'assets_processed': results['processed'],
            'success_rate': results['successful'] / results['processed'] if results['processed'] > 0 else 0,
            'assets_per_second': results['processed'] / total_time if total_time > 0 else 0,
            'cpu_usage_avg': (cpu_before + cpu_after) / 2,
            'memory_usage_avg': (memory_before + memory_after) / 2,
            'efficiency_score': (results['processed'] / total_time / worker_count) if total_time > 0 and worker_count > 0 else 0
        }
        
        print(f"   ⏱️  Time: {total_time:.1f}s | Rate: {benchmark_result['assets_per_second']:.2f} assets/sec | "
              f"Success: {benchmark_result['success_rate']*100:.1f}% | "
              f"CPU: {benchmark_result['cpu_usage_avg']:.1f}% | "
              f"Efficiency: {benchmark_result['efficiency_score']:.3f}")
        
        return benchmark_result
    
    def run_benchmark(self, source: str, sample_size: int = 20) -> List[Dict]:
        """Run benchmark across different worker counts"""
        print(f"🚀 Starting Worker Benchmark for {source.upper()}")
        print(f"📊 Sample Size: {sample_size} assets")
        print("=" * 60)
        
        self.print_system_info()
        
        # Get sample assets
        assets = self.get_sample_assets(source, sample_size)
        if not assets:
            return []
        
        # Determine worker counts to test based on system
        cpu_count = self.system_info['cpu_count']
        worker_counts = [1, 2, 4, 6, 8]
        
        # Add more worker counts based on CPU cores (your system has 14 cores)
        if cpu_count >= 10:
            worker_counts.extend([10, 12, 14])
        if cpu_count >= 14:
            worker_counts.extend([16, 18, 20])
        if cpu_count >= 16:
            worker_counts.extend([24, 28])
        
        # Don't exceed 2x CPU count (diminishing returns)
        worker_counts = [w for w in worker_counts if w <= cpu_count * 2]
        
        print(f"🧪 Testing worker counts: {worker_counts}")
        print(f"💡 Based on {cpu_count} CPU cores")
        print()
        
        # Run benchmarks
        results = []
        for worker_count in worker_counts:
            try:
                result = self.benchmark_worker_count(assets, worker_count)
                results.append(result)
                
                # Brief pause between tests
                time.sleep(2)
                
            except Exception as e:
                print(f"❌ Benchmark failed for {worker_count} workers: {e}")
        
        return results
    
    def analyze_results(self, results: List[Dict]):
        """Analyze benchmark results and provide recommendations"""
        if not results:
            print("❌ No benchmark results to analyze")
            return
        
        print("\n" + "=" * 60)
        print("📊 BENCHMARK RESULTS ANALYSIS")
        print("=" * 60)
        
        # Sort by efficiency score (assets per second per worker)
        results_by_efficiency = sorted(results, key=lambda x: x['efficiency_score'], reverse=True)
        results_by_speed = sorted(results, key=lambda x: x['assets_per_second'], reverse=True)
        
        print("\n🏆 Results by Total Speed (assets/second):")
        print(f"{'Workers':<8} {'Time(s)':<8} {'Rate':<12} {'Success%':<9} {'CPU%':<6} {'Memory%':<8} {'Efficiency':<10}")
        print("-" * 70)
        for result in results_by_speed:
            print(f"{result['workers']:<8} "
                  f"{result['total_time']:<8.1f} "
                  f"{result['assets_per_second']:<12.2f} "
                  f"{result['success_rate']*100:<9.1f} "
                  f"{result['cpu_usage_avg']:<6.1f} "
                  f"{result['memory_usage_avg']:<8.1f} "
                  f"{result['efficiency_score']:<10.3f}")
        
        print("\n🎯 Results by Efficiency (speed per worker):")
        print(f"{'Workers':<8} {'Efficiency':<12} {'Rate':<12} {'CPU%':<6} {'Recommendation'}")
        print("-" * 60)
        for result in results_by_efficiency:
            if result == results_by_efficiency[0]:
                rec = "⭐ MOST EFFICIENT"
            elif result['assets_per_second'] == max(r['assets_per_second'] for r in results):
                rec = "🚀 FASTEST"
            else:
                rec = ""
            
            print(f"{result['workers']:<8} "
                  f"{result['efficiency_score']:<12.3f} "
                  f"{result['assets_per_second']:<12.2f} "
                  f"{result['cpu_usage_avg']:<6.1f} "
                  f"{rec}")
        
        # Recommendations
        best_efficiency = results_by_efficiency[0]
        best_speed = results_by_speed[0]
        
        print("\n💡 RECOMMENDATIONS:")
        print("-" * 30)
        
        if best_efficiency['workers'] == best_speed['workers']:
            print(f"🎯 OPTIMAL: {best_efficiency['workers']} workers")
            print(f"   • Best balance of speed and efficiency")
            print(f"   • {best_efficiency['assets_per_second']:.2f} assets/second")
            print(f"   • {best_efficiency['efficiency_score']:.3f} efficiency score")
        else:
            print(f"⚡ FOR MAXIMUM SPEED: {best_speed['workers']} workers")
            print(f"   • {best_speed['assets_per_second']:.2f} assets/second")
            print(f"   • CPU usage: {best_speed['cpu_usage_avg']:.1f}%")
            
            print(f"\n💰 FOR BEST EFFICIENCY: {best_efficiency['workers']} workers")
            print(f"   • {best_efficiency['assets_per_second']:.2f} assets/second")
            print(f"   • {best_efficiency['efficiency_score']:.3f} efficiency per worker")
            print(f"   • Lower resource usage")
        
        # System-specific advice
        cpu_count = self.system_info['cpu_count']
        memory_gb = self.system_info['memory_gb']
        
        print(f"\n🖥️  SYSTEM-SPECIFIC ADVICE:")
        print(f"   • Your system: {cpu_count} CPU cores, {memory_gb}GB RAM")
        
        if memory_gb < 8:
            print(f"   ⚠️  Limited RAM - consider fewer workers to avoid memory pressure")
        elif memory_gb >= 16:
            print(f"   ✅ Plenty of RAM - can handle higher worker counts")
        
        if cpu_count >= 8:
            print(f"   ✅ Multi-core system - parallel processing will be very effective")
        else:
            print(f"   💡 Fewer cores - moderate worker counts will be most efficient")
        
        print(f"\n🚀 COMMAND TO USE:")
        recommended_workers = best_efficiency['workers']
        print(f"   python audio_fingerprint_processor.py --source artlist --workers {recommended_workers}")

def main():
    parser = argparse.ArgumentParser(description='Benchmark optimal worker count for your system')
    parser.add_argument('--source', choices=['artlist', 'motionarray'], required=True,
                       help='Source to benchmark (artlist or motionarray)')
    parser.add_argument('--sample-size', type=int, default=30,
                       help='Number of assets to test with (default: 30)')
    
    args = parser.parse_args()
    
    # Validate sample size
    if args.sample_size < 5:
        print("❌ Sample size too small. Minimum 5 assets required for reliable benchmarking.")
        return 1
    elif args.sample_size > 50:
        print("⚠️  Large sample size may take a long time. Consider using 10-30 for faster results.")
    
    try:
        benchmark = WorkerBenchmark()
        results = benchmark.run_benchmark(args.source, args.sample_size)
        benchmark.analyze_results(results)
        
        return 0
        
    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
