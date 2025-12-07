#!/usr/bin/env python3
"""
OCC vs MOCC Benchmark Plotter

Generates graphs from benchmark CSV data.

Usage:
    python3 plot_benchmark.py [benchmark_results.csv] [--output graph.png]
"""

import sys
import argparse
import csv
from pathlib import Path

# Try to import matplotlib, provide helpful error if not available
try:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

def read_csv(filename):
    """Read benchmark CSV data."""
    data = {
        'time': [],
        'occ_abort_rate': [],
        'mocc_abort_rate': [],
        'occ_committed': [],
        'mocc_committed': [],
        'mocc_locks': []
    }
    
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data['time'].append(float(row['time_sec']))
            data['occ_abort_rate'].append(float(row['occ_abort_rate']))
            data['mocc_abort_rate'].append(float(row['mocc_abort_rate']))
            data['occ_committed'].append(int(row['occ_committed']))
            data['mocc_committed'].append(int(row['mocc_committed']))
            data['mocc_locks'].append(int(row['mocc_locks']))
    
    return data

def plot_abort_rate(data, output_file):
    """Create abort rate over time graph."""
    
    # Set up the style
    plt.style.use('seaborn-v0_8-darkgrid' if 'seaborn-v0_8-darkgrid' in plt.style.available else 'ggplot')
    
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # Colors
    occ_color = '#E74C3C'   # Red
    mocc_color = '#27AE60'  # Green
    
    # Plot abort rates
    ax.plot(data['time'], data['occ_abort_rate'], 
            color=occ_color, linewidth=2.5, label='OCC (Pure Optimistic)',
            marker='o', markersize=4, markevery=5)
    
    ax.plot(data['time'], data['mocc_abort_rate'], 
            color=mocc_color, linewidth=2.5, label='MOCC (Mixed)',
            marker='s', markersize=4, markevery=5)
    
    # Fill under curves for visual effect
    ax.fill_between(data['time'], data['occ_abort_rate'], alpha=0.2, color=occ_color)
    ax.fill_between(data['time'], data['mocc_abort_rate'], alpha=0.2, color=mocc_color)
    
    # Labels and title
    ax.set_xlabel('Time (seconds)', fontsize=14, fontweight='bold')
    ax.set_ylabel('Abort Rate (%)', fontsize=14, fontweight='bold')
    ax.set_title('OCC vs MOCC: Abort Rate Over Time\n(High Contention Workload)', 
                 fontsize=16, fontweight='bold', pad=20)
    
    # Legend
    ax.legend(loc='upper right', fontsize=12, framealpha=0.9)
    
    # Grid
    ax.grid(True, alpha=0.3)
    ax.set_axisbelow(True)
    
    # Y-axis formatting
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=1))
    ax.set_ylim(bottom=0)
    
    # X-axis
    ax.set_xlim(left=0)
    
    # Add annotations for final values
    if data['time']:
        final_time = data['time'][-1]
        final_occ = data['occ_abort_rate'][-1]
        final_mocc = data['mocc_abort_rate'][-1]
        
        ax.annotate(f'{final_occ:.1f}%', 
                    xy=(final_time, final_occ),
                    xytext=(10, 0), textcoords='offset points',
                    fontsize=11, fontweight='bold', color=occ_color)
        
        ax.annotate(f'{final_mocc:.1f}%', 
                    xy=(final_time, final_mocc),
                    xytext=(10, 0), textcoords='offset points',
                    fontsize=11, fontweight='bold', color=mocc_color)
    
    # Add text box with summary
    if data['time']:
        reduction = final_occ - final_mocc
        textstr = f'MOCC reduces abort rate by {reduction:.1f} percentage points'
        props = dict(boxstyle='round', facecolor='wheat', alpha=0.8)
        ax.text(0.02, 0.98, textstr, transform=ax.transAxes, fontsize=11,
                verticalalignment='top', bbox=props)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    print(f"Graph saved to: {output_file}")
    
    # Also show the plot if running interactively
    plt.show()

def plot_combined(data, output_file):
    """Create a combined graph with abort rate and throughput."""
    
    plt.style.use('seaborn-v0_8-darkgrid' if 'seaborn-v0_8-darkgrid' in plt.style.available else 'ggplot')
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    
    # Colors
    occ_color = '#E74C3C'
    mocc_color = '#27AE60'
    
    # ===== Top plot: Abort Rate =====
    ax1.plot(data['time'], data['occ_abort_rate'], 
             color=occ_color, linewidth=2.5, label='OCC',
             marker='o', markersize=3, markevery=5)
    ax1.plot(data['time'], data['mocc_abort_rate'], 
             color=mocc_color, linewidth=2.5, label='MOCC',
             marker='s', markersize=3, markevery=5)
    
    ax1.fill_between(data['time'], data['occ_abort_rate'], alpha=0.2, color=occ_color)
    ax1.fill_between(data['time'], data['mocc_abort_rate'], alpha=0.2, color=mocc_color)
    
    ax1.set_ylabel('Abort Rate (%)', fontsize=12, fontweight='bold')
    ax1.set_title('OCC vs MOCC Performance Under High Contention', 
                  fontsize=14, fontweight='bold', pad=10)
    ax1.legend(loc='upper right', fontsize=10)
    ax1.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=1))
    ax1.set_ylim(bottom=0)
    ax1.grid(True, alpha=0.3)
    
    # ===== Bottom plot: Cumulative Committed Transactions =====
    ax2.plot(data['time'], data['occ_committed'], 
             color=occ_color, linewidth=2.5, label='OCC')
    ax2.plot(data['time'], data['mocc_committed'], 
             color=mocc_color, linewidth=2.5, label='MOCC')
    
    ax2.set_xlabel('Time (seconds)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Committed Transactions', fontsize=12, fontweight='bold')
    ax2.legend(loc='lower right', fontsize=10)
    ax2.grid(True, alpha=0.3)
    
    # Format y-axis with K/M suffixes
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, p: f'{x/1000:.0f}K' if x < 1000000 else f'{x/1000000:.1f}M'))
    
    plt.tight_layout()
    
    # Save with different name
    combined_output = output_file.replace('.png', '_combined.png')
    plt.savefig(combined_output, dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    print(f"Combined graph saved to: {combined_output}")
    
    plt.show()

def print_ascii_graph(data):
    """Print a simple ASCII graph for terminals without matplotlib."""
    
    print("\n" + "=" * 70)
    print("  ABORT RATE OVER TIME (ASCII Graph)")
    print("=" * 70)
    
    max_rate = max(max(data['occ_abort_rate']), max(data['mocc_abort_rate']), 1)
    height = 20
    width = min(50, len(data['time']))
    
    # Sample data if too many points
    step = max(1, len(data['time']) // width)
    times = data['time'][::step]
    occ_rates = data['occ_abort_rate'][::step]
    mocc_rates = data['mocc_abort_rate'][::step]
    
    print(f"\n  {'Abort Rate':^10} |")
    print(f"  {max_rate:>8.1f}% |", end="")
    
    for row in range(height, -1, -1):
        threshold = (row / height) * max_rate
        line = ""
        for i in range(len(times)):
            occ_above = occ_rates[i] >= threshold
            mocc_above = mocc_rates[i] >= threshold
            
            if occ_above and mocc_above:
                line += "█"  # Both
            elif occ_above:
                line += "▓"  # OCC only (red in concept)
            elif mocc_above:
                line += "░"  # MOCC only (green in concept)
            else:
                line += " "
        
        if row == height:
            print(line)
        elif row == height // 2:
            print(f"  {max_rate/2:>8.1f}% |{line}")
        elif row == 0:
            print(f"  {0:>8.1f}% |{line}")
            print(f"           +{'-' * len(times)}")
            print(f"            0{' ' * (len(times)//2 - 1)}Time (s){' ' * (len(times)//2 - 4)}{data['time'][-1]:.1f}")
        else:
            print(f"           |{line}")
    
    print("\n  Legend: ▓ = OCC, ░ = MOCC, █ = Both")
    print("\n  Final Abort Rates:")
    print(f"    OCC:  {data['occ_abort_rate'][-1]:.2f}%")
    print(f"    MOCC: {data['mocc_abort_rate'][-1]:.2f}%")
    print(f"    Reduction: {data['occ_abort_rate'][-1] - data['mocc_abort_rate'][-1]:.2f} percentage points")
    print("=" * 70 + "\n")

def main():
    parser = argparse.ArgumentParser(description='Plot OCC vs MOCC benchmark results')
    parser.add_argument('input_file', nargs='?', default='benchmark_results.csv',
                        help='Input CSV file (default: benchmark_results.csv)')
    parser.add_argument('-o', '--output', default='abort_rate_graph.png',
                        help='Output image file (default: abort_rate_graph.png)')
    parser.add_argument('--ascii', action='store_true',
                        help='Print ASCII graph instead of saving image')
    parser.add_argument('--combined', action='store_true',
                        help='Generate combined graph with throughput')
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not Path(args.input_file).exists():
        print(f"Error: Input file '{args.input_file}' not found.")
        print("\nTo generate benchmark data, run:")
        print("  ./build/benchmark_occ_vs_mocc")
        sys.exit(1)
    
    # Read data
    print(f"Reading data from: {args.input_file}")
    data = read_csv(args.input_file)
    print(f"Loaded {len(data['time'])} data points")
    
    if args.ascii or not HAS_MATPLOTLIB:
        if not HAS_MATPLOTLIB:
            print("\nNote: matplotlib not installed. Showing ASCII graph.")
            print("Install with: pip install matplotlib")
        print_ascii_graph(data)
    else:
        plot_abort_rate(data, args.output)
        if args.combined:
            plot_combined(data, args.output)

if __name__ == '__main__':
    main()


