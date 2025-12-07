/**
 * OCC vs MOCC Performance Benchmark
 * 
 * This benchmark compares OCC and MOCC implementations:
 * 
 * OCC:
 * - mdb::VersionedRow for per-column versioning (from src/memdb/row.h)
 * - Version-based validation (version_check)
 * 
 * MOCC:
 * - janus::TemperatureTracker (from src/deptran/mocc/temperature.h)
 * - janus::MoccLockManager (from src/deptran/mocc/mocc_lock.h)
 * - Adaptive locking: pessimistic for hot records, OCC for cold records
 * 
 * CPU Throttling:
 * - Simulates work to emulate larger systems on small machines
 * - Controlled via --throttle parameter (microseconds of work per transaction)
 */

#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <mutex>

// Include memdb types for OCC
#include "memdb/schema.h"
#include "memdb/row.h"
#include "memdb/value.h"
#include "memdb/txn_occ.h"

// Include MOCC implementation
#include "deptran/mocc/temperature.h"
#include "deptran/mocc/mocc_lock.h"

// Transaction ID type
using TxnId = uint64_t;

// Colors for output
#define GREEN "\033[32m"
#define RED "\033[31m"
#define YELLOW "\033[33m"
#define CYAN "\033[36m"
#define BOLD "\033[1m"
#define RESET "\033[0m"

namespace benchmark {

// ============================================================================
// CPU Throttling Configuration
// ============================================================================

// Global throttle setting (microseconds of simulated work per transaction)
std::atomic<int> g_throttle_us{0};

/**
 * Simulate CPU work to throttle throughput
 * This helps emulate a larger system on a small machine by:
 * 1. Reducing transaction throughput
 * 2. Increasing window for conflicts
 * 3. Making contention patterns more realistic
 */
void SimulateWork(int microseconds) {
  if (microseconds <= 0) return;
  
  // Use busy-wait for more accurate timing (sleep has ~1ms minimum granularity)
  auto start = std::chrono::high_resolution_clock::now();
  auto target = start + std::chrono::microseconds(microseconds);
  
  // Mix of busy-wait and yielding to be more realistic
  volatile uint64_t dummy = 0;
  while (std::chrono::high_resolution_clock::now() < target) {
    // Do some "work" to prevent optimization
    for (int i = 0; i < 100; i++) {
      dummy += i * i;
    }
    // Occasionally yield to other threads
    if ((dummy & 0xFF) == 0) {
      std::this_thread::yield();
    }
  }
}

// ============================================================================
// Time-Series Data Collection
// ============================================================================

struct TimePoint {
  double time_sec;
  uint64_t committed;
  uint64_t aborted;
  double abort_rate;
  uint64_t locks_acquired;
};

// Global atomic counters
std::atomic<uint64_t> g_committed{0};
std::atomic<uint64_t> g_aborted{0};
std::atomic<uint64_t> g_locks{0};
std::atomic<bool> g_running{false};

// ============================================================================
// Database using mdb::VersionedRow
// ============================================================================

class Database {
public:
  explicit Database(int num_records) : num_records_(num_records) {
    // Create schema with one integer column
    schema_ = new mdb::Schema();
    schema_->add_column("value", mdb::Value::I64, false);
    schema_->freeze();
    
    // Create versioned rows
    rows_.resize(num_records);
    for (int i = 0; i < num_records; i++) {
      std::vector<mdb::Value> values = { mdb::Value(static_cast<int64_t>(i)) };
      rows_[i] = mdb::VersionedRow::create(schema_, values);
    }
  }
  
  ~Database() {
    for (auto* row : rows_) {
      if (row) row->release();
    }
    delete schema_;
  }
  
  mdb::VersionedRow* GetRow(int id) { 
    return rows_[id % rows_.size()]; 
  }
  
  size_t Size() const { return rows_.size(); }
  
  // OCC version check - same logic as TxnOCC::version_check()
  bool VersionCheck(const std::vector<std::pair<mdb::VersionedRow*, mdb::version_t>>& read_set) {
    for (const auto& [row, saved_ver] : read_set) {
      mdb::version_t curr_ver = row->get_column_ver(0);
      if (curr_ver != saved_ver) {
        return false;  // Version changed - abort
      }
    }
    return true;
  }

private:
  int num_records_;
  mdb::Schema* schema_;
  std::vector<mdb::VersionedRow*> rows_;
};

// ============================================================================
// Access Pattern Generator
// ============================================================================

class AccessGenerator {
public:
  AccessGenerator(int num_records, double hot_fraction, double hot_prob)
    : num_records_(num_records),
      hot_records_(std::max(1, static_cast<int>(num_records * hot_fraction))),
      hot_probability_(hot_prob),
      gen_(std::random_device{}()),
      uniform_(0.0, 1.0),
      hot_dist_(0, hot_records_ - 1),
      cold_dist_(hot_records_, std::max(hot_records_, num_records - 1)) {}
  
  std::vector<int> GetAccessSet(int count) {
    std::vector<int> result;
    std::unordered_set<int> seen;
    while (result.size() < static_cast<size_t>(count)) {
      int r;
      if (uniform_(gen_) < hot_probability_) {
        r = hot_dist_(gen_);
      } else if (hot_records_ < num_records_) {
        r = cold_dist_(gen_);
      } else {
        r = hot_dist_(gen_);
      }
      if (seen.find(r) == seen.end()) {
        seen.insert(r);
        result.push_back(r);
      }
    }
    std::sort(result.begin(), result.end());
    return result;
  }

private:
  int num_records_;
  int hot_records_;
  double hot_probability_;
  std::mt19937 gen_;
  std::uniform_real_distribution<> uniform_;
  std::uniform_int_distribution<> hot_dist_;
  std::uniform_int_distribution<> cold_dist_;
};

// ============================================================================
// OCC Transaction Execution
// Uses mdb::VersionedRow::get_column_ver() and incr_column_ver()
// ============================================================================

bool ExecuteOCC(Database& db, const std::vector<int>& record_ids, TxnId txn_id) {
  // PHASE 1: READ - Save versions
  std::vector<std::pair<mdb::VersionedRow*, mdb::version_t>> read_set;
  read_set.reserve(record_ids.size());
  
  for (int id : record_ids) {
    mdb::VersionedRow* row = db.GetRow(id);
    mdb::version_t ver = row->get_column_ver(0);
    read_set.emplace_back(row, ver);
  }
  
  // Simulate work between read and validate (where conflicts can happen)
  SimulateWork(g_throttle_us.load(std::memory_order_relaxed) / 2);
  
  // PHASE 2: VALIDATE - Check versions haven't changed
  if (!db.VersionCheck(read_set)) {
    return false;  // ABORT
  }
  
  // PHASE 3: WRITE - Increment versions
  for (int id : record_ids) {
    mdb::VersionedRow* row = db.GetRow(id);
    row->incr_column_ver(0);
  }
  
  // Simulate remaining work after commit
  SimulateWork(g_throttle_us.load(std::memory_order_relaxed) / 2);
  
  return true;  // COMMIT
}

void RunOCCWorker(Database& db, int records_per_txn, 
                  std::atomic<TxnId>& txn_counter) {
  AccessGenerator gen(db.Size(), 0.05, 0.9);
  
  while (g_running.load(std::memory_order_acquire)) {
    TxnId txn_id = txn_counter.fetch_add(1, std::memory_order_relaxed);
    auto access_set = gen.GetAccessSet(records_per_txn);
    
    bool success = false;
    int retries = 0;
    
    while (!success && retries < 50 && g_running.load(std::memory_order_relaxed)) {
      success = ExecuteOCC(db, access_set, txn_id);
      if (!success) {
        g_aborted.fetch_add(1, std::memory_order_relaxed);
        retries++;
        std::this_thread::sleep_for(std::chrono::microseconds(1 << std::min(retries, 8)));
      }
    }
    
    if (success) {
      g_committed.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

// ============================================================================
// MOCC Transaction Execution  
// Uses janus::TemperatureTracker and janus::MoccLockManager
// ============================================================================

bool ExecuteMOCC(Database& db, const std::vector<int>& record_ids,
                 TxnId txn_id, bool is_retry) {
  auto& temp_tracker = janus::TemperatureTracker::Instance();
  auto& lock_manager = janus::MoccLockManager::Instance();
  
  std::vector<mdb::VersionedRow*> locked_rows;
  std::vector<std::pair<mdb::VersionedRow*, mdb::version_t>> occ_rows;
  
  // PHASE 1: CLASSIFY - Check temperature and decide OCC vs pessimistic
  for (int id : record_ids) {
    mdb::VersionedRow* row = db.GetRow(id);
    
    // Check temperature
    janus::TemperatureLevel level = temp_tracker.GetLevel(row, 0);
    
    if (level == janus::TemperatureLevel::HOT || is_retry) {
      // HOT - use lock manager with blocking acquisition
      janus::LockStatus status = lock_manager.AcquireLock(
          txn_id, row, 0, janus::LockMode::EXCLUSIVE, true);
      
      if (status != janus::LockStatus::ACQUIRED) {
        for (auto* r : locked_rows) {
          lock_manager.ReleaseLock(txn_id, r, 0);
        }
        return false;
      }
      
      locked_rows.push_back(row);
      g_locks.fetch_add(1, std::memory_order_relaxed);
    } else {
      // COLD/WARM - use OCC with version tracking
      mdb::version_t ver = row->get_column_ver(0);
      occ_rows.emplace_back(row, ver);
    }
    
    // Increment temperature
    temp_tracker.IncrementTemperature(row, 0, 1);
  }
  
  // Simulate work between read and validate
  SimulateWork(g_throttle_us.load(std::memory_order_relaxed) / 2);
  
  // PHASE 2: VALIDATE OCC rows
  bool valid = true;
  for (const auto& [row, saved_ver] : occ_rows) {
    mdb::version_t curr_ver = row->get_column_ver(0);
    if (curr_ver != saved_ver) {
      valid = false;
      temp_tracker.RecordAbort(row, 0);  // Heat up on conflict
      break;
    }
  }
  
  if (!valid) {
    for (auto* r : locked_rows) {
      lock_manager.ReleaseLock(txn_id, r, 0);
    }
    return false;
  }
  
  // PHASE 3: WRITE - Update versions and release locks
  for (auto* row : locked_rows) {
    row->incr_column_ver(0);
    temp_tracker.RecordCommit(row, 0);
    lock_manager.ReleaseLock(txn_id, row, 0);
  }
  
  for (const auto& [row, _] : occ_rows) {
    row->incr_column_ver(0);
    temp_tracker.RecordCommit(row, 0);
  }
  
  // Simulate remaining work after commit
  SimulateWork(g_throttle_us.load(std::memory_order_relaxed) / 2);
  
  return true;
}

void RunMOCCWorker(Database& db, int records_per_txn,
                   std::atomic<TxnId>& txn_counter) {
  AccessGenerator gen(db.Size(), 0.05, 0.9);
  
  while (g_running.load(std::memory_order_acquire)) {
    TxnId txn_id = txn_counter.fetch_add(1, std::memory_order_relaxed);
    auto access_set = gen.GetAccessSet(records_per_txn);
    
    bool success = false;
    int retries = 0;
    
    while (!success && retries < 50 && g_running.load(std::memory_order_relaxed)) {
      success = ExecuteMOCC(db, access_set, txn_id, retries > 0);
      if (!success) {
        g_aborted.fetch_add(1, std::memory_order_relaxed);
        retries++;
        std::this_thread::sleep_for(std::chrono::microseconds(1 << std::min(retries, 6)));
      }
    }
    
    if (success) {
      g_committed.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

// ============================================================================
// Time-Series Collection
// ============================================================================

std::vector<TimePoint> CollectTimeSeries(double duration_sec, double interval_sec) {
  std::vector<TimePoint> data;
  auto start = std::chrono::high_resolution_clock::now();
  double elapsed = 0;
  
  while (elapsed < duration_sec) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(static_cast<int>(interval_sec * 1000)));
    
    auto now = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration<double>(now - start).count();
    
    uint64_t committed = g_committed.load(std::memory_order_relaxed);
    uint64_t aborted = g_aborted.load(std::memory_order_relaxed);
    uint64_t locks = g_locks.load(std::memory_order_relaxed);
    
    TimePoint tp;
    tp.time_sec = elapsed;
    tp.committed = committed;
    tp.aborted = aborted;
    tp.abort_rate = (committed + aborted > 0) ? 
                    (aborted * 100.0) / (committed + aborted) : 0.0;
    tp.locks_acquired = locks;
    data.push_back(tp);
  }
  
  return data;
}

// ============================================================================
// Benchmark Runners
// ============================================================================

std::vector<TimePoint> RunOCCBenchmark(int num_threads, int num_records,
                                       int records_per_txn, double duration_sec) {
  Database db(num_records);
  std::vector<std::thread> threads;
  std::atomic<TxnId> txn_counter{1};
  
  g_committed = 0;
  g_aborted = 0;
  g_locks = 0;
  g_running = true;
  
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(RunOCCWorker, std::ref(db), records_per_txn, 
                         std::ref(txn_counter));
  }
  
  auto data = CollectTimeSeries(duration_sec, 0.1);
  
  g_running = false;
  for (auto& t : threads) {
    t.join();
  }
  
  return data;
}

std::vector<TimePoint> RunMOCCBenchmark(int num_threads, int num_records,
                                        int records_per_txn, double duration_sec) {
  Database db(num_records);
  std::vector<std::thread> threads;
  std::atomic<TxnId> txn_counter{1};
  
  // Clear MOCC singletons
  janus::TemperatureTracker::Instance().Clear();
  janus::MoccLockManager::Instance().Clear();
  
  g_committed = 0;
  g_aborted = 0;
  g_locks = 0;
  g_running = true;
  
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(RunMOCCWorker, std::ref(db), records_per_txn,
                         std::ref(txn_counter));
  }
  
  auto data = CollectTimeSeries(duration_sec, 0.1);
  
  g_running = false;
  for (auto& t : threads) {
    t.join();
  }
  
  return data;
}

// ============================================================================
// CSV Output
// ============================================================================

void WriteCSV(const std::string& filename,
              const std::vector<TimePoint>& occ_data,
              const std::vector<TimePoint>& mocc_data) {
  std::ofstream file(filename);
  file << "time_sec,occ_abort_rate,mocc_abort_rate,occ_committed,mocc_committed,mocc_locks\n";
  
  size_t max_size = std::max(occ_data.size(), mocc_data.size());
  for (size_t i = 0; i < max_size; i++) {
    double time = (i < occ_data.size()) ? occ_data[i].time_sec : mocc_data[i].time_sec;
    double occ_rate = (i < occ_data.size()) ? occ_data[i].abort_rate : 0;
    double mocc_rate = (i < mocc_data.size()) ? mocc_data[i].abort_rate : 0;
    uint64_t occ_committed = (i < occ_data.size()) ? occ_data[i].committed : 0;
    uint64_t mocc_committed = (i < mocc_data.size()) ? mocc_data[i].committed : 0;
    uint64_t mocc_locks = (i < mocc_data.size()) ? mocc_data[i].locks_acquired : 0;
    
    file << std::fixed << std::setprecision(3)
         << time << ","
         << occ_rate << ","
         << mocc_rate << ","
         << occ_committed << ","
         << mocc_committed << ","
         << mocc_locks << "\n";
  }
  
  file.close();
}

} // namespace benchmark

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
  std::cout << BOLD << CYAN << R"(
╔═══════════════════════════════════════════════════════════════════╗
║                                                                   ║
║                    OCC vs MOCC Benchmark                          ║
║                                                                   ║
║   OCC:  mdb::VersionedRow (version-based validation)              ║
║   MOCC: TemperatureTracker + MoccLockManager (adaptive locking)   ║
║                                                                   ║
╚═══════════════════════════════════════════════════════════════════╝
)" << RESET << "\n";

  int num_threads = 4;
  int num_records = 100;
  int records_per_txn = 10;
  double duration_sec = 5.0;
  int throttle_us = 0;
  std::string output_file = "benchmark_results.csv";
  
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "-o" && i + 1 < argc) output_file = argv[++i];
    else if (arg == "-t" && i + 1 < argc) num_threads = std::stoi(argv[++i]);
    else if (arg == "-d" && i + 1 < argc) duration_sec = std::stod(argv[++i]);
    else if (arg == "-r" && i + 1 < argc) num_records = std::stoi(argv[++i]);
    else if (arg == "-n" && i + 1 < argc) records_per_txn = std::stoi(argv[++i]);
    else if ((arg == "-s" || arg == "--throttle") && i + 1 < argc) throttle_us = std::stoi(argv[++i]);
    else if (arg == "-h" || arg == "--help") {
      std::cout << "Usage: " << argv[0] << " [options]\n\n";
      std::cout << "Options:\n";
      std::cout << "  -t <threads>     Number of threads (default: 4)\n";
      std::cout << "  -r <records>     Number of records in database (default: 100)\n";
      std::cout << "  -n <count>       Records accessed per transaction (default: 10)\n";
      std::cout << "  -d <duration>    Duration in seconds (default: 5)\n";
      std::cout << "  -s, --throttle <us>  CPU throttle per transaction in microseconds (default: 0)\n";
      std::cout << "  -o <file>        Output CSV file (default: benchmark_results.csv)\n";
      std::cout << "  -h, --help       Show this help message\n";
      std::cout << "\nCPU Throttling:\n";
      std::cout << "  The --throttle option simulates additional work per transaction.\n";
      std::cout << "  This helps emulate larger systems on small machines by:\n";
      std::cout << "    - Reducing transaction throughput\n";
      std::cout << "    - Increasing the window for conflicts\n";
      std::cout << "    - Making contention patterns more realistic\n";
      std::cout << "\n  Suggested values:\n";
      std::cout << "    0      - No throttling (maximum speed)\n";
      std::cout << "    100    - Light throttle (simulates ~10 threads on 4 cores)\n";
      std::cout << "    500    - Medium throttle (simulates ~20 threads)\n";
      std::cout << "    1000   - Heavy throttle (simulates ~40 threads)\n";
      std::cout << "    5000   - Very heavy throttle (simulates distributed system)\n";
      return 0;
    }
  }
  
  // Set global throttle
  benchmark::g_throttle_us.store(throttle_us, std::memory_order_relaxed);
  
  std::cout << "Configuration:\n";
  std::cout << "  Threads:            " << num_threads << "\n";
  std::cout << "  Records:            " << num_records << "\n";
  std::cout << "  Records per txn:    " << records_per_txn << "\n";
  std::cout << "  Duration:           " << duration_sec << " sec\n";
  std::cout << "  CPU Throttle:       " << throttle_us << " µs/txn";
  if (throttle_us > 0) {
    int simulated_threads = num_threads * (1 + throttle_us / 100);
    std::cout << " (simulates ~" << simulated_threads << " threads)";
  }
  std::cout << "\n";
  std::cout << "  Output file:        " << output_file << "\n\n";
  
  std::cout << "Running OCC benchmark..." << std::flush;
  auto occ_data = benchmark::RunOCCBenchmark(num_threads, num_records, 
                                              records_per_txn, duration_sec);
  std::cout << " Done! (" << occ_data.size() << " samples)\n";
  
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  
  std::cout << "Running MOCC benchmark..." << std::flush;
  auto mocc_data = benchmark::RunMOCCBenchmark(num_threads, num_records,
                                                records_per_txn, duration_sec);
  std::cout << " Done! (" << mocc_data.size() << " samples)\n";
  
  benchmark::WriteCSV(output_file, occ_data, mocc_data);
  std::cout << "\nResults written to: " << output_file << "\n";
  
  // Summary
  std::cout << "\n" << CYAN;
  std::cout << "═══════════════════════════════════════════════════════════════════\n";
  std::cout << "                           SUMMARY                                  \n";
  std::cout << "═══════════════════════════════════════════════════════════════════\n" << RESET;
  
  if (!occ_data.empty() && !mocc_data.empty()) {
    auto& occ_final = occ_data.back();
    auto& mocc_final = mocc_data.back();
    
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\nFinal Statistics:\n";
    std::cout << "  " << RED << "OCC:  " << RESET 
              << occ_final.committed << " committed, "
              << occ_final.abort_rate << "% abort rate\n";
    std::cout << "  " << GREEN << "MOCC: " << RESET 
              << mocc_final.committed << " committed, "
              << mocc_final.abort_rate << "% abort rate\n";
    std::cout << "        " << mocc_final.locks_acquired << " locks acquired\n";
    
    auto temp_stats = janus::TemperatureTracker::Instance().GetStats();
    std::cout << "\n  Temperature Stats:\n";
    std::cout << "    Hot records:  " << temp_stats.hot_records << "\n";
    std::cout << "    Warm records: " << temp_stats.warm_records << "\n";
    std::cout << "    Cold records: " << temp_stats.cold_records << "\n";
  }
  
  std::cout << "\n" << GREEN << "To generate graph: python3 scripts/plot_benchmark.py " 
            << output_file << RESET << "\n\n";
  
  return 0;
}
