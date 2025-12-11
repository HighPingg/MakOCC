/**
 * MOCC Performance Benchmark
 * 
 * Benchmarks for measuring the performance of MOCC implementation:
 * - Temperature tracking overhead
 * - Lock manager throughput
 * - Hybrid locking (cold vs hot records)
 * - Multi-threaded contention scenarios
 */

#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <iomanip>
#include <cstring>

// Include MOCC components
#include "../src/deptran/mocc/temperature.h"
#include "../src/deptran/mocc/mocc_lock.h"
#include "../src/deptran/mocc/logical_clock.h"

using namespace std;
using namespace janus;

// ============================================================================
// Benchmark Configuration
// ============================================================================

constexpr int WARMUP_ITERATIONS = 1000;
constexpr int BENCHMARK_ITERATIONS = 100000;
constexpr int NUM_RECORDS = 1000;
constexpr int NUM_THREADS = 4;
constexpr int CONTENTION_HOT_RECORDS = 10;  // Number of hot records for contention

// Color codes
#define GREEN "\033[32m"
#define YELLOW "\033[33m"
#define CYAN "\033[36m"
#define RESET "\033[0m"

// ============================================================================
// Benchmark Utilities
// ============================================================================

struct BenchmarkResult {
    string name;
    double ops_per_sec;
    double avg_latency_ns;
    double p50_latency_ns;
    double p99_latency_ns;
    size_t total_ops;
    double duration_ms;
};

class BenchTimer {
public:
    void start() {
        start_time_ = chrono::high_resolution_clock::now();
    }
    
    double elapsed_ns() const {
        auto end = chrono::high_resolution_clock::now();
        return chrono::duration<double, nano>(end - start_time_).count();
    }
    
    double elapsed_ms() const {
        return elapsed_ns() / 1e6;
    }
    
    double elapsed_sec() const {
        return elapsed_ns() / 1e9;
    }

private:
    chrono::high_resolution_clock::time_point start_time_;
};

void print_result(const BenchmarkResult& result) {
    cout << CYAN << "  " << result.name << RESET << endl;
    cout << "    Throughput: " << GREEN << fixed << setprecision(0) 
         << result.ops_per_sec << " ops/sec" << RESET << endl;
    cout << "    Avg Latency: " << fixed << setprecision(1) 
         << result.avg_latency_ns << " ns" << endl;
    if (result.p50_latency_ns > 0) {
        cout << "    P50 Latency: " << result.p50_latency_ns << " ns" << endl;
        cout << "    P99 Latency: " << result.p99_latency_ns << " ns" << endl;
    }
    cout << "    Total Ops: " << result.total_ops 
         << " in " << fixed << setprecision(2) << result.duration_ms << " ms" << endl;
}

void print_separator() {
    cout << string(60, '-') << endl;
}

// ============================================================================
// Benchmark 1: Temperature Tracking Overhead
// ============================================================================

BenchmarkResult benchmark_temperature_lookup() {
    TemperatureTracker::Instance().Clear();
    
    // Pre-populate some records
    vector<void*> rows;
    for (int i = 0; i < NUM_RECORDS; i++) {
        void* row = reinterpret_cast<void*>(0x10000 + i * 0x1000);
        rows.push_back(row);
        // Initialize with some records having temperature
        if (i % 10 == 0) {
            TemperatureTracker::Instance().RecordAbort(row, 0);
        }
    }
    
    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        TemperatureTracker::Instance().GetLevel(rows[i % NUM_RECORDS], 0);
    }
    
    // Benchmark
    BenchTimer timer;
    timer.start();
    
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
        TemperatureTracker::Instance().GetLevel(rows[i % NUM_RECORDS], 0);
    }
    
    double elapsed_ns = timer.elapsed_ns();
    
    BenchmarkResult result;
    result.name = "Temperature Lookup";
    result.total_ops = BENCHMARK_ITERATIONS;
    result.duration_ms = elapsed_ns / 1e6;
    result.ops_per_sec = (BENCHMARK_ITERATIONS / elapsed_ns) * 1e9;
    result.avg_latency_ns = elapsed_ns / BENCHMARK_ITERATIONS;
    result.p50_latency_ns = 0;
    result.p99_latency_ns = 0;
    
    return result;
}

BenchmarkResult benchmark_temperature_update() {
    TemperatureTracker::Instance().Clear();
    
    vector<void*> rows;
    for (int i = 0; i < NUM_RECORDS; i++) {
        void* row = reinterpret_cast<void*>(0x20000 + i * 0x1000);
        rows.push_back(row);
    }
    
    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        TemperatureTracker::Instance().RecordAbort(rows[i % NUM_RECORDS], 0);
    }
    TemperatureTracker::Instance().Clear();
    
    // Benchmark
    BenchTimer timer;
    timer.start();
    
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
        TemperatureTracker::Instance().RecordAbort(rows[i % NUM_RECORDS], 0);
    }
    
    double elapsed_ns = timer.elapsed_ns();
    
    BenchmarkResult result;
    result.name = "Temperature Update (RecordAbort)";
    result.total_ops = BENCHMARK_ITERATIONS;
    result.duration_ms = elapsed_ns / 1e6;
    result.ops_per_sec = (BENCHMARK_ITERATIONS / elapsed_ns) * 1e9;
    result.avg_latency_ns = elapsed_ns / BENCHMARK_ITERATIONS;
    result.p50_latency_ns = 0;
    result.p99_latency_ns = 0;
    
    return result;
}

// ============================================================================
// Benchmark 2: Lock Manager Throughput
// ============================================================================

BenchmarkResult benchmark_lock_acquire_release() {
    MoccLockManager::Instance().Clear();
    
    vector<void*> rows;
    for (int i = 0; i < NUM_RECORDS; i++) {
        void* row = reinterpret_cast<void*>(0x30000 + i * 0x1000);
        rows.push_back(row);
    }
    
    txnid_t base_txn = 1000;
    
    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        txnid_t txn = base_txn + i;
        void* row = rows[i % NUM_RECORDS];
        MoccLockManager::Instance().TryAcquireLock(txn, row, 0, LockMode::EXCLUSIVE);
        MoccLockManager::Instance().ReleaseLock(txn, row, 0);
    }
    MoccLockManager::Instance().Clear();
    
    // Benchmark
    BenchTimer timer;
    timer.start();
    
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
        txnid_t txn = base_txn + WARMUP_ITERATIONS + i;
        void* row = rows[i % NUM_RECORDS];
        MoccLockManager::Instance().TryAcquireLock(txn, row, 0, LockMode::EXCLUSIVE);
        MoccLockManager::Instance().ReleaseLock(txn, row, 0);
    }
    
    double elapsed_ns = timer.elapsed_ns();
    
    BenchmarkResult result;
    result.name = "Lock Acquire + Release (Exclusive)";
    result.total_ops = BENCHMARK_ITERATIONS * 2;  // acquire + release
    result.duration_ms = elapsed_ns / 1e6;
    result.ops_per_sec = (BENCHMARK_ITERATIONS * 2 / elapsed_ns) * 1e9;
    result.avg_latency_ns = elapsed_ns / (BENCHMARK_ITERATIONS * 2);
    result.p50_latency_ns = 0;
    result.p99_latency_ns = 0;
    
    return result;
}

BenchmarkResult benchmark_lock_shared() {
    MoccLockManager::Instance().Clear();
    
    vector<void*> rows;
    for (int i = 0; i < NUM_RECORDS; i++) {
        void* row = reinterpret_cast<void*>(0x40000 + i * 0x1000);
        rows.push_back(row);
    }
    
    txnid_t base_txn = 100000;
    
    // Benchmark shared lock acquisition
    BenchTimer timer;
    timer.start();
    
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
        txnid_t txn = base_txn + i;
        void* row = rows[i % NUM_RECORDS];
        MoccLockManager::Instance().TryAcquireLock(txn, row, 0, LockMode::SHARED);
        MoccLockManager::Instance().ReleaseLock(txn, row, 0);
    }
    
    double elapsed_ns = timer.elapsed_ns();
    
    BenchmarkResult result;
    result.name = "Lock Acquire + Release (Shared)";
    result.total_ops = BENCHMARK_ITERATIONS * 2;
    result.duration_ms = elapsed_ns / 1e6;
    result.ops_per_sec = (BENCHMARK_ITERATIONS * 2 / elapsed_ns) * 1e9;
    result.avg_latency_ns = elapsed_ns / (BENCHMARK_ITERATIONS * 2);
    result.p50_latency_ns = 0;
    result.p99_latency_ns = 0;
    
    return result;
}

// ============================================================================
// Benchmark 3: Logical Clock Operations
// ============================================================================

BenchmarkResult benchmark_logical_clock() {
    LogicalClock clock(1);
    
    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        clock.GetTimestamp();
    }
    
    // Benchmark
    BenchTimer timer;
    timer.start();
    
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
        clock.GetTimestamp();
    }
    
    double elapsed_ns = timer.elapsed_ns();
    
    BenchmarkResult result;
    result.name = "Logical Clock GetTimestamp";
    result.total_ops = BENCHMARK_ITERATIONS;
    result.duration_ms = elapsed_ns / 1e6;
    result.ops_per_sec = (BENCHMARK_ITERATIONS / elapsed_ns) * 1e9;
    result.avg_latency_ns = elapsed_ns / BENCHMARK_ITERATIONS;
    result.p50_latency_ns = 0;
    result.p99_latency_ns = 0;
    
    return result;
}

// ============================================================================
// Benchmark 4: Multi-threaded Contention
// ============================================================================

atomic<size_t> total_ops_completed{0};
atomic<size_t> total_aborts{0};

void contention_worker(int thread_id, vector<void*>& hot_rows, 
                       vector<void*>& cold_rows, int iterations) {
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<> hot_dist(0, CONTENTION_HOT_RECORDS - 1);
    uniform_int_distribution<> cold_dist(0, NUM_RECORDS - 1);
    uniform_int_distribution<> choice(0, 99);  // 0-99 for percentage
    
    txnid_t base_txn = 1000000 + thread_id * 1000000;
    
    for (int i = 0; i < iterations; i++) {
        txnid_t txn = base_txn + i;
        
        // 30% hot, 70% cold access pattern
        void* row;
        bool is_hot = (choice(gen) < 30);
        
        if (is_hot) {
            row = hot_rows[hot_dist(gen)];
        } else {
            row = cold_rows[cold_dist(gen)];
        }
        
        // Try to acquire lock
        LockStatus status = MoccLockManager::Instance().TryAcquireLock(
            txn, row, 0, LockMode::EXCLUSIVE);
        
        if (status == LockStatus::ACQUIRED) {
            // Simulate some work
            volatile int x = 0;
            for (int j = 0; j < 10; j++) x++;
            
            MoccLockManager::Instance().ReleaseLock(txn, row, 0);
            total_ops_completed++;
        } else {
            // Record abort for temperature tracking
            TemperatureTracker::Instance().RecordAbort(row, 0);
            total_aborts++;
        }
    }
}

BenchmarkResult benchmark_multithreaded_contention() {
    MoccLockManager::Instance().Clear();
    TemperatureTracker::Instance().Clear();
    total_ops_completed = 0;
    total_aborts = 0;
    
    // Create hot rows (high contention)
    vector<void*> hot_rows;
    for (int i = 0; i < CONTENTION_HOT_RECORDS; i++) {
        void* row = reinterpret_cast<void*>(0x50000 + i * 0x1000);
        hot_rows.push_back(row);
    }
    
    // Create cold rows (low contention)
    vector<void*> cold_rows;
    for (int i = 0; i < NUM_RECORDS; i++) {
        void* row = reinterpret_cast<void*>(0x60000 + i * 0x1000);
        cold_rows.push_back(row);
    }
    
    int iterations_per_thread = BENCHMARK_ITERATIONS / NUM_THREADS;
    
    BenchTimer timer;
    timer.start();
    
    vector<thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back(contention_worker, t, ref(hot_rows), 
                            ref(cold_rows), iterations_per_thread);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    double elapsed_ns = timer.elapsed_ns();
    size_t total = total_ops_completed.load();
    size_t aborts = total_aborts.load();
    
    BenchmarkResult result;
    result.name = "Multi-threaded Contention (" + to_string(NUM_THREADS) + " threads)";
    result.total_ops = total;
    result.duration_ms = elapsed_ns / 1e6;
    result.ops_per_sec = (total / elapsed_ns) * 1e9;
    result.avg_latency_ns = elapsed_ns / (total + aborts);
    result.p50_latency_ns = 0;
    result.p99_latency_ns = 0;
    
    cout << "    Aborts: " << aborts << " (" 
         << fixed << setprecision(1) << (100.0 * aborts / (total + aborts)) 
         << "%)" << endl;
    
    return result;
}

// ============================================================================
// Benchmark 5: MOCC Hybrid Decision Making
// ============================================================================

BenchmarkResult benchmark_hybrid_decision() {
    TemperatureTracker::Instance().Clear();
    
    // Create records with different temperatures
    vector<void*> rows;
    for (int i = 0; i < NUM_RECORDS; i++) {
        void* row = reinterpret_cast<void*>(0x70000 + i * 0x1000);
        rows.push_back(row);
        
        // Make some records hot
        if (i < NUM_RECORDS / 10) {
            for (uint32_t j = 0; j < MOCC_HOT_THRESHOLD + 5; j++) {
                TemperatureTracker::Instance().RecordAbort(row, 0);
            }
        }
    }
    
    // Benchmark the decision-making (hot vs cold check)
    BenchTimer timer;
    timer.start();
    
    size_t hot_count = 0;
    size_t cold_count = 0;
    
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
        void* row = rows[i % NUM_RECORDS];
        TemperatureLevel level = TemperatureTracker::Instance().GetLevel(row, 0);
        
        if (level == TemperatureLevel::HOT) {
            hot_count++;
        } else {
            cold_count++;
        }
    }
    
    double elapsed_ns = timer.elapsed_ns();
    
    BenchmarkResult result;
    result.name = "Hybrid Decision (Hot/Cold Check)";
    result.total_ops = BENCHMARK_ITERATIONS;
    result.duration_ms = elapsed_ns / 1e6;
    result.ops_per_sec = (BENCHMARK_ITERATIONS / elapsed_ns) * 1e9;
    result.avg_latency_ns = elapsed_ns / BENCHMARK_ITERATIONS;
    result.p50_latency_ns = 0;
    result.p99_latency_ns = 0;
    
    cout << "    Hot decisions: " << hot_count << ", Cold decisions: " << cold_count << endl;
    
    return result;
}

// ============================================================================
// Main
// ============================================================================

int main() {
    cout << endl;
    cout << GREEN << "╔════════════════════════════════════════════════════════╗" << RESET << endl;
    cout << GREEN << "║         MOCC Performance Benchmark Suite               ║" << RESET << endl;
    cout << GREEN << "╚════════════════════════════════════════════════════════╝" << RESET << endl;
    cout << endl;
    
    cout << "Configuration:" << endl;
    cout << "  Iterations: " << BENCHMARK_ITERATIONS << endl;
    cout << "  Records: " << NUM_RECORDS << endl;
    cout << "  Threads: " << NUM_THREADS << endl;
    cout << "  Hot threshold: " << MOCC_HOT_THRESHOLD << endl;
    cout << endl;
    
    vector<BenchmarkResult> results;
    
    // Temperature Benchmarks
    cout << YELLOW << "=== Temperature Tracking Benchmarks ===" << RESET << endl;
    print_separator();
    results.push_back(benchmark_temperature_lookup());
    print_result(results.back());
    
    results.push_back(benchmark_temperature_update());
    print_result(results.back());
    cout << endl;
    
    // Lock Manager Benchmarks
    cout << YELLOW << "=== Lock Manager Benchmarks ===" << RESET << endl;
    print_separator();
    results.push_back(benchmark_lock_acquire_release());
    print_result(results.back());
    
    results.push_back(benchmark_lock_shared());
    print_result(results.back());
    cout << endl;
    
    // Logical Clock Benchmarks
    cout << YELLOW << "=== Logical Clock Benchmarks ===" << RESET << endl;
    print_separator();
    results.push_back(benchmark_logical_clock());
    print_result(results.back());
    cout << endl;
    
    // Multi-threaded Benchmarks
    cout << YELLOW << "=== Multi-threaded Benchmarks ===" << RESET << endl;
    print_separator();
    results.push_back(benchmark_multithreaded_contention());
    print_result(results.back());
    cout << endl;
    
    // Hybrid Decision Benchmarks
    cout << YELLOW << "=== Hybrid Decision Benchmarks ===" << RESET << endl;
    print_separator();
    results.push_back(benchmark_hybrid_decision());
    print_result(results.back());
    cout << endl;
    
    // Summary
    cout << GREEN << "=== Benchmark Summary ===" << RESET << endl;
    print_separator();
    
    double total_ops = 0;
    for (const auto& r : results) {
        total_ops += r.total_ops;
        cout << "  " << left << setw(40) << r.name 
             << right << setw(12) << fixed << setprecision(0) 
             << r.ops_per_sec << " ops/sec" << endl;
    }
    
    cout << endl;
    cout << GREEN << "Benchmark completed successfully!" << RESET << endl;
    cout << "Total operations: " << fixed << setprecision(0) << total_ops << endl;
    cout << endl;
    
    // Print temperature stats
    auto temp_stats = TemperatureTracker::Instance().GetStats();
    cout << "Temperature Stats:" << endl;
    cout << "  Total pages tracked: " << temp_stats.total_records << endl;
    cout << "  Hot pages: " << temp_stats.hot_records << endl;
    cout << "  Warm pages: " << temp_stats.warm_records << endl;
    cout << "  Cold pages: " << temp_stats.cold_records << endl;
    
    // Print lock stats
    auto lock_stats = MoccLockManager::Instance().GetStats();
    cout << endl;
    cout << "Lock Stats:" << endl;
    cout << "  Total locks tracked: " << lock_stats.total_locks << endl;
    cout << "  Currently held: " << lock_stats.held_locks << endl;
    
    return 0;
}
