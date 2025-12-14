#!/bin/bash
#
# Mako MOCC Benchmark Script
# 
# Measures throughput and abort rate of Mako with MOCC concurrency control.
# 
# Usage:
#   ./examples/benchmark_mako.sh [OPTIONS]
#
# Options:
#   --threads N       Number of worker threads (default: 4)
#   --duration N      Runtime in seconds (default: 10)
#   --warehouses N    Number of warehouses (default: same as threads)
#   --help            Show this help message
#

set -e

# Default parameters
THREADS=4
DURATION=10
WAREHOUSES=""
SHARD_INDEX=0
CLUSTER="localhost"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --threads)
            THREADS="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --warehouses)
            WAREHOUSES="$2"
            shift 2
            ;;
        --help)
            head -16 "$0" | tail -14
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Default warehouses to thread count if not specified
WAREHOUSES=${WAREHOUSES:-$THREADS}

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Shard config
SHARD_CONFIG="src/mako/config/local-shards1-warehouses${WAREHOUSES}.yml"

echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║                    Mako MOCC Benchmark                            ║"
echo "╠═══════════════════════════════════════════════════════════════════╣"
echo "║  Threads:     $THREADS"
echo "║  Duration:    ${DURATION}s"
echo "║  Warehouses:  $WAREHOUSES"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""

# Check if dbtest exists
if [ ! -f "./build/dbtest" ]; then
    echo "ERROR: ./build/dbtest not found. Please run 'make' first."
    exit 1
fi

# Check if config files exist
if [ ! -f "$SHARD_CONFIG" ]; then
    echo "WARNING: $SHARD_CONFIG not found, using default config"
    SHARD_CONFIG="src/mako/config/local-shards1.yml"
fi

echo "═══════════════════════════════════════════════════════════════════"
echo "  Running Mako Benchmark..."
echo "═══════════════════════════════════════════════════════════════════"

# Build command
CMD="./build/dbtest --num-threads $THREADS --shard-index $SHARD_INDEX"
CMD="$CMD --shard-config $SHARD_CONFIG"
CMD="$CMD -F config/1leader_2followers/paxos${THREADS}_shardidx${SHARD_INDEX}.yml"
CMD="$CMD -P $CLUSTER"

echo "Command: $CMD"
echo ""

# Run benchmark with timeout
LOG_FILE="mako_benchmark.log"
timeout $((DURATION + 60)) $CMD 2>&1 | tee "$LOG_FILE" || true

# Extract metrics from log
echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Extracting Results..."
echo "═══════════════════════════════════════════════════════════════════"

COMMITS=$(grep "n_commits:" "$LOG_FILE" | tail -1 | awk '{print $2}' || echo "0")
ABORT_RATE=$(grep "agg_abort_rate:" "$LOG_FILE" | awk '{print $2}' || echo "0")
THROUGHPUT=$(grep "agg_throughput:" "$LOG_FILE" | awk '{print $2}' || echo "0")
AVG_LATENCY=$(grep "avg_latency:" "$LOG_FILE" | awk '{print $2}' || echo "0")
PERSIST_THROUGHPUT=$(grep "agg_persist_throughput:" "$LOG_FILE" | awk '{print $2}' || echo "0")

# Default values if not found
COMMITS=${COMMITS:-0}
ABORT_RATE=${ABORT_RATE:-0}
THROUGHPUT=${THROUGHPUT:-0}
AVG_LATENCY=${AVG_LATENCY:-0}
PERSIST_THROUGHPUT=${PERSIST_THROUGHPUT:-0}

# Print summary
echo ""
echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║                      BENCHMARK RESULTS                            ║"
echo "╠═══════════════════════════════════════════════════════════════════╣"
printf "║  %-25s  %-35s ║\n" "Total Commits:" "$COMMITS"
printf "║  %-25s  %-35s ║\n" "Throughput:" "$THROUGHPUT ops/sec"
printf "║  %-25s  %-35s ║\n" "Persist Throughput:" "$PERSIST_THROUGHPUT ops/sec"
printf "║  %-25s  %-35s ║\n" "Abort Rate:" "$ABORT_RATE aborts/sec"
printf "║  %-25s  %-35s ║\n" "Avg Latency:" "$AVG_LATENCY ms"
echo "╚═══════════════════════════════════════════════════════════════════╝"

# Save to CSV
OUTPUT_CSV="mako_benchmark_results.csv"
echo "threads,warehouses,commits,throughput,abort_rate,avg_latency,persist_throughput" > "$OUTPUT_CSV"
echo "$THREADS,$WAREHOUSES,$COMMITS,$THROUGHPUT,$ABORT_RATE,$AVG_LATENCY,$PERSIST_THROUGHPUT" >> "$OUTPUT_CSV"

echo ""
echo "Results saved to: $OUTPUT_CSV"
echo "Full log: $LOG_FILE"
