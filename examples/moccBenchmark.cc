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
constexpr int NUM_THREADS = 12;
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
// Benchmark 6: TPC-C NewOrder Simulation
// ============================================================================

// TPC-C Constants
constexpr int TPCC_NUM_WAREHOUSES = 100; // Default, can be overridden
constexpr int TPCC_DISTRICTS_PER_W = 10;
constexpr int TPCC_CUSTOMERS_PER_D = 3000;
constexpr int TPCC_ITEMS = 100000;

class TpccKeyGenerator {
public:
    // Base addresses for different tables to ensure no overlap
    static constexpr uintptr_t BASE_WAREHOUSE = 0x100000000;
    static constexpr uintptr_t BASE_DISTRICT  = 0x200000000;
    static constexpr uintptr_t BASE_CUSTOMER  = 0x300000000;
    static constexpr uintptr_t BASE_ITEM      = 0x400000000;
    static constexpr uintptr_t BASE_STOCK     = 0x500000000;

    static void* GetWarehouseKey(int w_id) {
        return reinterpret_cast<void*>(BASE_WAREHOUSE + w_id * 64);
    }

    static void* GetDistrictKey(int w_id, int d_id) {
        return reinterpret_cast<void*>(BASE_DISTRICT + (w_id * TPCC_DISTRICTS_PER_W + d_id) * 64);
    }

    static void* GetCustomerKey(int w_id, int d_id, int c_id) {
        // Simplified mapping for customer, might overlap if not careful but sufficient for benchmark
        uint64_t idx = (uint64_t)w_id * TPCC_DISTRICTS_PER_W * TPCC_CUSTOMERS_PER_D + 
                       (uint64_t)d_id * TPCC_CUSTOMERS_PER_D + c_id;
        return reinterpret_cast<void*>(BASE_CUSTOMER + idx * 64);
    }

    static void* GetItemKey(int i_id) {
        return reinterpret_cast<void*>(BASE_ITEM + i_id * 64);
    }

    static void* GetStockKey(int w_id, int i_id) {
        uint64_t idx = (uint64_t)w_id * TPCC_ITEMS + i_id;
        return reinterpret_cast<void*>(BASE_STOCK + idx * 64);
    }
};

atomic<size_t> tpcc_ops_completed{0};
atomic<size_t> tpcc_aborts{0};

void tpcc_worker(int thread_id, int num_warehouses, int iterations) {
    random_device rd;
    mt19937 gen(rd());
    
    // TPC-C NewOrder parameters
    uniform_int_distribution<> w_dist(0, num_warehouses - 1);
    uniform_int_distribution<> d_dist(0, TPCC_DISTRICTS_PER_W - 1);
    uniform_int_distribution<> c_dist(0, TPCC_CUSTOMERS_PER_D - 1);
    uniform_int_distribution<> i_dist(0, TPCC_ITEMS - 1);
    uniform_int_distribution<> ol_cnt_dist(5, 15);
    uniform_int_distribution<> remote_dist(0, 99); // 1% remote
    
    txnid_t base_txn = 2000000 + thread_id * 1000000;
    
    for (int i = 0; i < iterations; i++) {
        txnid_t txn = base_txn + i;
        bool aborted = false;
        vector<void*> acquired_locks;
        
        // 1. Warehouse and District
        int w_id = thread_id % num_warehouses; // Home warehouse
        int d_id = d_dist(gen);
        int c_id = c_dist(gen);
        
        void* w_key = TpccKeyGenerator::GetWarehouseKey(w_id);
        void* d_key = TpccKeyGenerator::GetDistrictKey(w_id, d_id);
        void* c_key = TpccKeyGenerator::GetCustomerKey(w_id, d_id, c_id);
        
        // Acquire locks (simulated)
        // In real TPC-C, W is read, D is read/write, C is read
        
        // Warehouse (Read)
        TemperatureTracker::Instance().GetLevel(w_key, 0); // Check temperature
        if (MoccLockManager::Instance().TryAcquireLock(txn, w_key, 0, LockMode::SHARED) != LockStatus::ACQUIRED) {
            aborted = true;
            TemperatureTracker::Instance().RecordAbort(w_key, 0);
        } else {
            acquired_locks.push_back(w_key);
            
            // District (Read/Write - update next_o_id)
            TemperatureTracker::Instance().GetLevel(d_key, 0); // Check temperature
            if (MoccLockManager::Instance().TryAcquireLock(txn, d_key, 0, LockMode::EXCLUSIVE) != LockStatus::ACQUIRED) {
                aborted = true;
                TemperatureTracker::Instance().RecordAbort(d_key, 0);
            } else {
                acquired_locks.push_back(d_key);
                
                // Customer (Read)
                TemperatureTracker::Instance().GetLevel(c_key, 0); // Check temperature
                if (MoccLockManager::Instance().TryAcquireLock(txn, c_key, 0, LockMode::SHARED) != LockStatus::ACQUIRED) {
                    aborted = true;
                    TemperatureTracker::Instance().RecordAbort(c_key, 0);
                } else {
                    acquired_locks.push_back(c_key);
                    
                    // Order lines
                    int ol_cnt = ol_cnt_dist(gen);
                    for (int j = 0; j < ol_cnt; j++) {
                        int i_id = i_dist(gen);
                        int supply_w_id = w_id;
                        
                        // 1% remote transaction
                        if (remote_dist(gen) == 0 && num_warehouses > 1) {
                            do {
                                supply_w_id = w_dist(gen);
                            } while (supply_w_id == w_id);
                        }
                        
                        void* i_key = TpccKeyGenerator::GetItemKey(i_id);
                        void* s_key = TpccKeyGenerator::GetStockKey(supply_w_id, i_id);
                        
                        // Item (Read)
                        TemperatureTracker::Instance().GetLevel(i_key, 0); // Check temperature
                        if (MoccLockManager::Instance().TryAcquireLock(txn, i_key, 0, LockMode::SHARED) != LockStatus::ACQUIRED) {
                            aborted = true;
                            TemperatureTracker::Instance().RecordAbort(i_key, 0);
                            break;
                        }
                        acquired_locks.push_back(i_key);
                        
                        // Stock (Read/Write)
                        TemperatureTracker::Instance().GetLevel(s_key, 0); // Check temperature
                        if (MoccLockManager::Instance().TryAcquireLock(txn, s_key, 0, LockMode::EXCLUSIVE) != LockStatus::ACQUIRED) {
                            aborted = true;
                            TemperatureTracker::Instance().RecordAbort(s_key, 0);
                            break;
                        }
                        acquired_locks.push_back(s_key);
                    }
                }
            }
        }
        
        if (aborted) {
            // Abort: release all locks and record abort
            for (void* lock : acquired_locks) {
                MoccLockManager::Instance().ReleaseLock(txn, lock, 0);
            }
            // Record abort on the last failed key (simplified, we don't know exactly which one failed here easily without refactoring)
            // For now just count aborts
            tpcc_aborts++;
        } else {
            // Simulate CPU throttle (work) inside the critical section to increase contention
            // 100000 iterations to ensure noticeable delay
            volatile int x = 0;
            for (int k = 0; k < 100000; k++) {
                x++;
            }
            
            // Commit: release all locks
            for (void* lock : acquired_locks) {
                MoccLockManager::Instance().ReleaseLock(txn, lock, 0);
            }
            tpcc_ops_completed++;
        }
    }
}

BenchmarkResult benchmark_tpcc_new_order() {
    MoccLockManager::Instance().Clear();
    TemperatureTracker::Instance().Clear();
    tpcc_ops_completed = 0;
    tpcc_aborts = 0;
    
    int num_warehouses = TPCC_NUM_WAREHOUSES; // Scale warehouses with threads
    int iterations_per_thread = BENCHMARK_ITERATIONS / NUM_THREADS;
    
    cout << "    Running TPC-C NewOrder with " << num_warehouses << " warehouses..." << endl;
    
    BenchTimer timer;
    timer.start();
    
    vector<thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back(tpcc_worker, t, num_warehouses, iterations_per_thread);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    double elapsed_ns = timer.elapsed_ns();
    size_t total = tpcc_ops_completed.load();
    size_t aborts = tpcc_aborts.load();
    
    BenchmarkResult result;
    result.name = "TPC-C NewOrder Simulation";
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

    // TPC-C Benchmarks
    cout << YELLOW << "=== TPC-C Benchmarks ===" << RESET << endl;
    print_separator();
    results.push_back(benchmark_tpcc_new_order());
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
