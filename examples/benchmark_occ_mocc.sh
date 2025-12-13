#!/bin/bash
#
# OCC vs MOCC Benchmark Script
# 
# Runs dbtest with both OCC and MOCC configurations and compares results.
# 
# Usage:
#   ./examples/benchmark_occ_mocc.sh [OPTIONS]
#
# Options:
#   --threads N       Number of worker threads (default: 4)
#   --duration N      Runtime in seconds (default: 10)
#   --contention MODE Contention level: 'low' or 'high' (default: low)
#   --output FILE     Output CSV file (default: benchmark_results.csv)
#   --help            Show this help message
#

set -e

# Default parameters
THREADS=4
DURATION=10
CONTENTION="low"
OUTPUT="benchmark_results.csv"
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
        --contention)
            CONTENTION="$2"
            shift 2
            ;;
        --output)
            OUTPUT="$2"
            shift 2
            ;;
        --help)
            head -18 "$0" | tail -16
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Set warehouses based on contention level
# High contention: 1 warehouse total (all threads compete for same data)
# Low contention: many warehouses (threads work on separate data)
if [ "$CONTENTION" == "high" ]; then
    WAREHOUSES=1
    CONFIG_SUFFIX="high_contention"
else
    WAREHOUSES=$THREADS
    CONFIG_SUFFIX="low_contention"
fi

echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║                    OCC vs MOCC Benchmark                          ║"
echo "╠═══════════════════════════════════════════════════════════════════╣"
echo "║  Threads:     $THREADS"
echo "║  Duration:    ${DURATION}s"
echo "║  Contention:  $CONTENTION ($WAREHOUSES warehouse(s))"
echo "║  Output:      $OUTPUT"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""

# Generate shard config for this benchmark
SHARD_CONFIG="src/mako/config/local-shards1-warehouses${WAREHOUSES}.yml"

# Function to run benchmark and extract metrics
run_benchmark() {
    local CC_MODE=$1
    local CONFIG_FILE=$2
    local LOG_FILE=$3
    
    echo "Running $CC_MODE benchmark..."
    
    # Build command
    CMD="./build/dbtest --num-threads $THREADS --shard-index $SHARD_INDEX"
    CMD="$CMD --shard-config $SHARD_CONFIG"
    CMD="$CMD -F config/1leader_2followers/paxos${THREADS}_shardidx${SHARD_INDEX}.yml"
    CMD="$CMD -F $CONFIG_FILE"
    CMD="$CMD -P $CLUSTER"
    
    # Run with timeout
    timeout $((DURATION + 30)) $CMD 2>&1 | tee "$LOG_FILE" || true
    
    # Extract metrics from log
    local COMMITS=$(grep "n_commits:" "$LOG_FILE" | tail -1 | awk '{print $2}')
    local ABORTS=$(grep "agg_abort_rate:" "$LOG_FILE" | awk '{print $2}')
    local THROUGHPUT=$(grep "agg_throughput:" "$LOG_FILE" | awk '{print $2}')
    
    # Default values if not found
    COMMITS=${COMMITS:-0}
    ABORTS=${ABORTS:-0}
    THROUGHPUT=${THROUGHPUT:-0}
    
    echo "$CC_MODE,$COMMITS,$ABORTS,$THROUGHPUT"
}

# Create output CSV header
echo "mode,commits,abort_rate,throughput" > "$OUTPUT"

# Run OCC benchmark
echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Running OCC Benchmark"
echo "═══════════════════════════════════════════════════════════════════"
OCC_RESULT=$(run_benchmark "OCC" "config/occ_paxos.yml" "occ_benchmark.log")
echo "$OCC_RESULT" >> "$OUTPUT"
OCC_COMMITS=$(echo "$OCC_RESULT" | cut -d',' -f2)
OCC_ABORTS=$(echo "$OCC_RESULT" | cut -d',' -f3)
OCC_THROUGHPUT=$(echo "$OCC_RESULT" | cut -d',' -f4)

# Brief pause between runs
sleep 2

# Run MOCC benchmark
echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Running MOCC Benchmark"
echo "═══════════════════════════════════════════════════════════════════"
MOCC_RESULT=$(run_benchmark "MOCC" "config/mocc.yml" "mocc_benchmark.log")
echo "$MOCC_RESULT" >> "$OUTPUT"
MOCC_COMMITS=$(echo "$MOCC_RESULT" | cut -d',' -f2)
MOCC_ABORTS=$(echo "$MOCC_RESULT" | cut -d',' -f3)
MOCC_THROUGHPUT=$(echo "$MOCC_RESULT" | cut -d',' -f4)

# Print summary
echo ""
echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║                         RESULTS SUMMARY                           ║"
echo "╠═══════════════════════════════════════════════════════════════════╣"
printf "║  %-10s  %-15s  %-15s  %-15s ║\n" "Mode" "Commits" "Abort Rate" "Throughput"
echo "╠═══════════════════════════════════════════════════════════════════╣"
printf "║  %-10s  %-15s  %-15s  %-15s ║\n" "OCC" "$OCC_COMMITS" "$OCC_ABORTS/s" "$OCC_THROUGHPUT ops/s"
printf "║  %-10s  %-15s  %-15s  %-15s ║\n" "MOCC" "$MOCC_COMMITS" "$MOCC_ABORTS/s" "$MOCC_THROUGHPUT ops/s"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""
echo "Results saved to: $OUTPUT"
echo "Logs: occ_benchmark.log, mocc_benchmark.log"
