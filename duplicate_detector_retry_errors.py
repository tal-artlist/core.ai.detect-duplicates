#!/usr/bin/env python3
"""
Duplicate Detector - Error Analysis and Retry Tool

This script helps you analyze and retry failed comparisons from duplicate_detector.py runs.

Features:
1. Analyze error files to understand failure patterns
2. Retry failed comparisons (transient issues, network problems, etc.)
3. Generate statistics by error type and source pair
4. Filter and retry specific error types

Usage:
    # Analyze errors without retrying
    python duplicate_detector_retry_errors.py duplicate_results_cross-source_20250116_143022_errors.jsonl --analyze-only
    
    # Retry all errors (requires Snowflake connection)
    python duplicate_detector_retry_errors.py duplicate_results_cross-source_20250116_143022_errors.jsonl --output retried_results.jsonl
    
    # Filter by error type before retrying
    python duplicate_detector_retry_errors.py errors.jsonl --filter-type COMPARISON_FAILED --output retried.jsonl
"""

import json
import argparse
import logging
from pathlib import Path
from collections import defaultdict
from typing import Dict, List
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_error_file(error_file: str) -> Dict:
    """Analyze error file and return statistics"""
    logger.info(f"📊 Analyzing error file: {error_file}")
    
    if not Path(error_file).exists():
        logger.error(f"❌ Error file not found: {error_file}")
        return {}
    
    stats = {
        'total_errors': 0,
        'by_error_type': defaultdict(int),
        'by_source_pair': defaultdict(int),
        'errors': []
    }
    
    try:
        with open(error_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    error = json.loads(line.strip())
                    stats['total_errors'] += 1
                    stats['by_error_type'][error.get('error_type', 'UNKNOWN')] += 1
                    
                    # Track source pair (e.g., artlist <-> motionarray)
                    source_pair = f"{error.get('source_1', 'unknown')} <-> {error.get('source_2', 'unknown')}"
                    stats['by_source_pair'][source_pair] += 1
                    
                    stats['errors'].append(error)
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️  Skipping invalid JSON at line {line_num}: {e}")
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Failed to analyze error file: {e}")
        return {}

def print_error_analysis(stats: Dict):
    """Print error analysis statistics"""
    if not stats:
        logger.error("❌ No statistics to display")
        return
    
    print("\n" + "="*80)
    print("📊 ERROR ANALYSIS")
    print("="*80)
    print(f"Total errors: {stats['total_errors']:,}")
    
    print("\n📈 By Error Type:")
    for error_type, count in sorted(stats['by_error_type'].items(), key=lambda x: x[1], reverse=True):
        pct = (count / stats['total_errors'] * 100) if stats['total_errors'] > 0 else 0
        print(f"  {error_type}: {count:,} ({pct:.1f}%)")
    
    print("\n📈 By Source Pair:")
    for source_pair, count in sorted(stats['by_source_pair'].items(), key=lambda x: x[1], reverse=True):
        pct = (count / stats['total_errors'] * 100) if stats['total_errors'] > 0 else 0
        print(f"  {source_pair}: {count:,} ({pct:.1f}%)")
    
    print("="*80)

def filter_errors(errors: List[Dict], error_type: str = None) -> List[Dict]:
    """Filter errors by type"""
    if not error_type:
        return errors
    
    filtered = [e for e in errors if e.get('error_type') == error_type]
    logger.info(f"🔍 Filtered {len(filtered):,} errors of type '{error_type}' from {len(errors):,} total")
    return filtered

def retry_errors(errors: List[Dict], output_file: str):
    """Retry failed comparisons (requires DuplicateDetector)"""
    logger.info(f"🔄 Retrying {len(errors):,} failed comparisons...")
    logger.info(f"📝 Results will be written to: {output_file}")
    
    # Import here to avoid circular dependency
    try:
        from duplicate_detector import DuplicateDetector
    except ImportError as e:
        logger.error(f"❌ Failed to import DuplicateDetector: {e}")
        return
    
    # Initialize detector with output file
    detector = DuplicateDetector(max_workers=4, output_file=output_file)
    
    try:
        success_count = 0
        still_failed_count = 0
        
        for i, error in enumerate(errors, 1):
            # Reconstruct the song dictionaries
            song1 = {
                'asset_id': error['asset_id_1'],
                'file_key': error['file_key_1'],
                'format': error['format_1'],
                'source': error['source_1'],
                'duration': error['duration_1']
            }
            
            song2 = {
                'asset_id': error['asset_id_2'],
                'file_key': error['file_key_2'],
                'format': error['format_2'],
                'source': error['source_2'],
                'duration': error['duration_2']
            }
            
            # Retry the comparison
            result = detector.compare_and_store_pair((song1, song2), similarity_threshold=0.0)
            
            if result is not None:
                success_count += 1
            else:
                still_failed_count += 1
            
            if i % 100 == 0:
                logger.info(f"📊 Progress: {i}/{len(errors)} | Success: {success_count} | Still failed: {still_failed_count}")
        
        # Flush remaining records
        detector.flush_file_buffer()
        detector.flush_error_buffer()
        
        logger.info(f"✅ Retry complete!")
        logger.info(f"   Successfully retried: {success_count:,}")
        logger.info(f"   Still failed: {still_failed_count:,}")
        logger.info(f"   Results written to: {output_file}")
        
        if still_failed_count > 0:
            error_file = output_file.replace('.jsonl', '_errors.jsonl')
            logger.warning(f"⚠️  New errors written to: {error_file}")
        
    finally:
        detector.close()

def main():
    parser = argparse.ArgumentParser(
        description='Error Analysis and Retry Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('error_file', help='Path to error JSONL file')
    parser.add_argument('--analyze-only', action='store_true',
                       help='Only analyze errors, do not retry')
    parser.add_argument('--output', '--output-file', dest='output_file',
                       help='Output file for retry results (JSONL format)')
    parser.add_argument('--filter-type', dest='filter_type',
                       help='Only process errors of this type (e.g., COMPARISON_FAILED, EXCEPTION)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.analyze_only and not args.output_file:
        parser.error("Must specify --output-file when retrying (or use --analyze-only)")
    
    # Analyze the error file
    stats = analyze_error_file(args.error_file)
    
    if not stats:
        return
    
    # Print analysis
    print_error_analysis(stats)
    
    if args.analyze_only:
        logger.info("✅ Analysis complete (--analyze-only specified, not retrying)")
        return
    
    # Filter errors if requested
    errors_to_retry = stats['errors']
    if args.filter_type:
        errors_to_retry = filter_errors(errors_to_retry, args.filter_type)
    
    if not errors_to_retry:
        logger.warning("⚠️  No errors to retry after filtering")
        return
    
    # Retry the errors
    retry_errors(errors_to_retry, args.output_file)

if __name__ == "__main__":
    main()

