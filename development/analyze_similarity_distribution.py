#!/usr/bin/env python3
"""
Analyze Similarity Distribution - Show counts, percentages, and plots of similarity scores
Usage: python analyze_similarity_distribution.py [--buckets 0.0,0.1,0.2,...] [--plot]
"""

import json
import argparse
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend

def load_and_analyze(jsonl_file, bucket_thresholds):
    """Load pairs and analyze similarity distribution"""
    print(f"📂 Loading pairs from {jsonl_file}...")
    
    # Create buckets
    buckets = []
    for i in range(len(bucket_thresholds) - 1):
        buckets.append((bucket_thresholds[i], bucket_thresholds[i + 1]))
    
    bucketed = {bucket: [] for bucket in buckets}
    
    total_pairs = 0
    skipped = 0
    similarities = []
    
    with open(jsonl_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                pair = json.loads(line)
                similarity = pair['similarity']
                similarities.append(similarity)
                total_pairs += 1
                
                # Find appropriate bucket
                for bucket_min, bucket_max in buckets:
                    if bucket_min <= similarity < bucket_max:
                        bucketed[(bucket_min, bucket_max)].append(similarity)
                        break
                    # Special case: include 1.0 in the last bucket
                    elif similarity == bucket_max == buckets[-1][1]:
                        bucketed[(bucket_min, bucket_max)].append(similarity)
                        break
                        
            except json.JSONDecodeError as e:
                skipped += 1
                continue
    
    print(f"   Loaded {total_pairs:,} total pairs")
    if skipped > 0:
        print(f"   ⚠️  Skipped {skipped:,} malformed lines")
    
    return bucketed, total_pairs, similarities

def print_statistics(bucketed, total_pairs):
    """Print detailed statistics about the distribution"""
    print(f"\n{'='*80}")
    print(f"📊 SIMILARITY DISTRIBUTION ANALYSIS")
    print(f"{'='*80}")
    print(f"Total pairs analyzed: {total_pairs:,}\n")
    
    # Table header
    print(f"{'Bucket':>15} | {'Count':>12} | {'Percentage':>12} | {'Bar Chart'}")
    print(f"{'-'*15}-+-{'-'*12}-+-{'-'*12}-+-{'-'*40}")
    
    # Sort by bucket range
    sorted_buckets = sorted(bucketed.items())
    
    for (bucket_min, bucket_max), values in sorted_buckets:
        count = len(values)
        percentage = (count / total_pairs * 100) if total_pairs > 0 else 0
        
        # Create a simple ASCII bar chart (max 40 chars)
        bar_length = int(percentage / 2.5)  # Scale to fit in 40 chars
        bar = '█' * bar_length
        
        bucket_label = f"[{bucket_min:.1f}-{bucket_max:.1f})"
        print(f"{bucket_label:>15} | {count:>12,} | {percentage:>11.2f}% | {bar}")
    
    print(f"{'-'*15}-+-{'-'*12}-+-{'-'*12}-+-{'-'*40}")
    print(f"{'TOTAL':>15} | {total_pairs:>12,} | {100.0:>11.2f}% |")
    
    # Key insights
    print(f"\n{'='*80}")
    print(f"🔍 KEY INSIGHTS:")
    print(f"{'='*80}")
    
    # Most common bucket
    max_bucket = max(sorted_buckets, key=lambda x: len(x[1]))
    max_count = len(max_bucket[1])
    max_pct = (max_count / total_pairs * 100)
    print(f"📈 Most common range: [{max_bucket[0][0]:.1f}-{max_bucket[0][1]:.1f}) with {max_count:,} pairs ({max_pct:.1f}%)")
    
    # Exact duplicates (1.0)
    exact_bucket = None
    for (bucket_min, bucket_max), values in sorted_buckets:
        if bucket_min == 1.0 or bucket_max == 1.1:
            exact_bucket = (bucket_min, bucket_max, len(values))
            break
    
    if exact_bucket:
        exact_pct = (exact_bucket[2] / total_pairs * 100)
        print(f"🎯 Exact duplicates (≥1.0): {exact_bucket[2]:,} pairs ({exact_pct:.1f}%)")
    
    # High similarity (>=0.8)
    high_sim_count = sum(len(values) for (bmin, bmax), values in sorted_buckets if bmin >= 0.8)
    high_sim_pct = (high_sim_count / total_pairs * 100)
    print(f"⚡ High similarity (≥0.8): {high_sim_count:,} pairs ({high_sim_pct:.1f}%)")
    
    # Low similarity (<0.5)
    low_sim_count = sum(len(values) for (bmin, bmax), values in sorted_buckets if bmax <= 0.5)
    low_sim_pct = (low_sim_count / total_pairs * 100)
    print(f"📉 Low similarity (<0.5): {low_sim_count:,} pairs ({low_sim_pct:.1f}%)")
    
    print(f"{'='*80}\n")

def create_plot(bucketed, total_pairs, output_file):
    """Create a bar chart of the distribution"""
    print(f"📊 Creating plot...")
    
    sorted_buckets = sorted(bucketed.items())
    
    # Prepare data
    labels = [f"{bmin:.1f}-{bmax:.1f}" for (bmin, bmax), _ in sorted_buckets]
    counts = [len(values) for _, values in sorted_buckets]
    percentages = [(count / total_pairs * 100) for count in counts]
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    fig.suptitle('Similarity Score Distribution', fontsize=16, fontweight='bold')
    
    # Plot 1: Counts
    bars1 = ax1.bar(range(len(labels)), counts, color='steelblue', alpha=0.8, edgecolor='black')
    ax1.set_xlabel('Similarity Range', fontsize=12)
    ax1.set_ylabel('Count', fontsize=12)
    ax1.set_title('Number of Pairs per Similarity Bucket', fontsize=14)
    ax1.set_xticks(range(len(labels)))
    ax1.set_xticklabels(labels, rotation=45, ha='right')
    ax1.grid(axis='y', alpha=0.3)
    
    # Add count labels on bars
    for i, (bar, count) in enumerate(zip(bars1, counts)):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{count:,}',
                ha='center', va='bottom', fontsize=9)
    
    # Plot 2: Percentages
    bars2 = ax2.bar(range(len(labels)), percentages, color='coral', alpha=0.8, edgecolor='black')
    ax2.set_xlabel('Similarity Range', fontsize=12)
    ax2.set_ylabel('Percentage (%)', fontsize=12)
    ax2.set_title('Percentage of Total Pairs per Similarity Bucket', fontsize=14)
    ax2.set_xticks(range(len(labels)))
    ax2.set_xticklabels(labels, rotation=45, ha='right')
    ax2.grid(axis='y', alpha=0.3)
    
    # Add percentage labels on bars
    for i, (bar, pct) in enumerate(zip(bars2, percentages)):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{pct:.1f}%',
                ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"   ✅ Plot saved to: {output_file}")
    
    # Also create a cumulative distribution plot
    cumulative_file = output_file.replace('.png', '_cumulative.png')
    create_cumulative_plot(sorted_buckets, total_pairs, cumulative_file)

def create_cumulative_plot(sorted_buckets, total_pairs, output_file):
    """Create a cumulative distribution plot"""
    labels = [f"{bmin:.1f}" for (bmin, bmax), _ in sorted_buckets]
    counts = [len(values) for _, values in sorted_buckets]
    
    # Calculate cumulative percentages
    cumulative = []
    running_total = 0
    for count in counts:
        running_total += count
        cumulative.append((running_total / total_pairs * 100))
    
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(range(len(labels)), cumulative, marker='o', linewidth=2, markersize=8, color='darkgreen')
    ax.fill_between(range(len(labels)), cumulative, alpha=0.3, color='lightgreen')
    
    ax.set_xlabel('Similarity Threshold', fontsize=12)
    ax.set_ylabel('Cumulative Percentage (%)', fontsize=12)
    ax.set_title('Cumulative Distribution of Similarity Scores', fontsize=14)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.grid(True, alpha=0.3)
    ax.set_ylim(0, 105)
    
    # Add reference lines
    ax.axhline(y=50, color='red', linestyle='--', alpha=0.5, label='50th percentile')
    ax.axhline(y=90, color='orange', linestyle='--', alpha=0.5, label='90th percentile')
    ax.legend()
    
    # Add percentage labels at key points
    for i, (x, y) in enumerate(zip(range(len(labels)), cumulative)):
        if i % 2 == 0:  # Label every other point to avoid crowding
            ax.text(x, y + 2, f'{y:.1f}%', ha='center', fontsize=8)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"   ✅ Cumulative plot saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description='Analyze and visualize similarity score distribution',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--input',
        type=str,
        default='duplicate_results_all_20251017_124125.jsonl',
        help='Input JSONL file (default: duplicate_results_all_20251017_124125.jsonl)'
    )
    
    parser.add_argument(
        '--buckets',
        type=str,
        default='0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,1.1',
        help='Comma-separated similarity thresholds (default: 0.0,0.1,0.2,...,1.0,1.1)'
    )
    
    parser.add_argument(
        '--plot',
        type=str,
        default='analysis/results/similarity_distribution.png',
        help='Output plot file (default: analysis/results/similarity_distribution.png)'
    )
    
    parser.add_argument(
        '--no-plot',
        action='store_true',
        help='Skip creating plots'
    )
    
    args = parser.parse_args()
    
    # Parse bucket thresholds
    bucket_thresholds = [float(x) for x in args.buckets.split(',')]
    bucket_thresholds.sort()
    
    print("📈 Similarity Distribution Analyzer")
    print("=" * 80)
    
    # Load and analyze
    bucketed, total_pairs, similarities = load_and_analyze(args.input, bucket_thresholds)
    
    # Print statistics
    print_statistics(bucketed, total_pairs)
    
    # Create plot if requested
    if not args.no_plot:
        import os
        os.makedirs(os.path.dirname(args.plot), exist_ok=True)
        create_plot(bucketed, total_pairs, args.plot)
    
    print(f"\n✅ Analysis complete!")

if __name__ == "__main__":
    main()

