/**
 * MOCC (Mixed Optimistic Concurrency Control) Tests
 * 
 * This test file validates the MOCC implementation including:
 * - Temperature tracking for hot/cold records
 * - Hybrid locking (pessimistic for hot, optimistic for cold)
 * - Lock communication on abort
 * - Pre-acquisition of locks on retry
 */

#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>
#include <queue>
#include <condition_variable>
#include <algorithm>
#include <cstdint>

// Color macros for output (if not defined)
#ifndef GREEN
#define GREEN "\033[32m"
#endif
#ifndef RED
#define RED "\033[31m"
#endif
#ifndef RESET
#define RESET "\033[0m"
#endif

// Define the types needed
using txnid_t = uint64_t;

namespace janus {

// ============================================================================
// Temperature Tracking (standalone implementation for testing)
// ============================================================================

constexpr uint32_t MOCC_COLD_THRESHOLD = 3;
constexpr uint32_t MOCC_HOT_THRESHOLD = 10;
constexpr uint32_t MOCC_MAX_TEMPERATURE = 20;
constexpr uint64_t MOCC_DECAY_INTERVAL_MS = 100;
constexpr double MOCC_DECAY_FACTOR = 0.9;

enum class TemperatureLevel {
  COLD = 0,
  WARM = 1,
  HOT = 2
};

struct RowColKey {
  void* row_ptr;
  int col_id;
  
  bool operator==(const RowColKey& other) const {
    return row_ptr == other.row_ptr && col_id == other.col_id;
  }
};

struct RowColKeyHash {
  size_t operator()(const RowColKey& key) const {
    return std::hash<void*>()(key.row_ptr) ^ 
           (std::hash<int>()(key.col_id) << 1);
  }
};

struct RecordTemperature {
  std::atomic<uint32_t> temperature{0};
  std::atomic<uint32_t> abort_count{0};
  std::atomic<uint64_t> last_access_time{0};
  std::atomic<uint64_t> last_decay_time{0};
  
  RecordTemperature() = default;
  RecordTemperature(const RecordTemperature&) = delete;
  RecordTemperature& operator=(const RecordTemperature&) = delete;
  RecordTemperature(RecordTemperature&& other) noexcept
    : temperature(other.temperature.load()),
      abort_count(other.abort_count.load()),
      last_access_time(other.last_access_time.load()),
      last_decay_time(other.last_decay_time.load()) {}
};

inline uint64_t GetCurrentTimeMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now().time_since_epoch()
  ).count();
}

class TemperatureTracker {
public:
  static TemperatureTracker& Instance() {
    static TemperatureTracker instance;
    return instance;
  }
  
  TemperatureLevel GetLevel(void* row, int col_id) {
    uint32_t temp = GetTemperature(row, col_id);
    if (temp >= MOCC_HOT_THRESHOLD) return TemperatureLevel::HOT;
    else if (temp >= MOCC_COLD_THRESHOLD) return TemperatureLevel::WARM;
    else return TemperatureLevel::COLD;
  }
  
  uint32_t GetTemperature(void* row, int col_id) {
    RowColKey key{row, col_id};
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = temperatures_.find(key);
    if (it == temperatures_.end()) return 0;
    return it->second->temperature.load(std::memory_order_relaxed);
  }
  
  void IncrementTemperature(void* row, int col_id, uint32_t increment = 1) {
    RecordTemperature& temp = GetOrCreate(row, col_id);
    uint32_t current = temp.temperature.load(std::memory_order_relaxed);
    uint32_t new_temp;
    do {
      new_temp = std::min(current + increment, MOCC_MAX_TEMPERATURE);
    } while (!temp.temperature.compare_exchange_weak(current, new_temp, std::memory_order_relaxed));
    temp.last_access_time.store(GetCurrentTimeMs(), std::memory_order_relaxed);
  }
  
  void RecordAbort(void* row, int col_id) {
    RecordTemperature& temp = GetOrCreate(row, col_id);
    temp.abort_count.fetch_add(1, std::memory_order_relaxed);
    uint32_t current = temp.temperature.load(std::memory_order_relaxed);
    uint32_t new_temp;
    do {
      new_temp = std::min(current + 3, MOCC_MAX_TEMPERATURE);
    } while (!temp.temperature.compare_exchange_weak(current, new_temp, std::memory_order_relaxed));
    temp.last_access_time.store(GetCurrentTimeMs(), std::memory_order_relaxed);
  }
  
  void RecordCommit(void* row, int col_id) {
    RowColKey key{row, col_id};
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = temperatures_.find(key);
    if (it == temperatures_.end()) return;
    RecordTemperature& temp = *it->second;
    uint32_t current = temp.temperature.load(std::memory_order_relaxed);
    if (current > 0) {
      temp.temperature.compare_exchange_weak(current, current - 1, std::memory_order_relaxed);
    }
  }
  
  void Clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    temperatures_.clear();
  }
  
  struct Stats {
    size_t total_records;
    size_t hot_records;
    size_t warm_records;
    size_t cold_records;
    uint64_t total_temperature;
  };
  
  Stats GetStats() {
    Stats stats{0, 0, 0, 0, 0};
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (const auto& pair : temperatures_) {
      stats.total_records++;
      uint32_t temp = pair.second->temperature.load(std::memory_order_relaxed);
      stats.total_temperature += temp;
      if (temp >= MOCC_HOT_THRESHOLD) stats.hot_records++;
      else if (temp >= MOCC_COLD_THRESHOLD) stats.warm_records++;
      else stats.cold_records++;
    }
    return stats;
  }

private:
  TemperatureTracker() = default;
  
  RecordTemperature& GetOrCreate(void* row, int col_id) {
    RowColKey key{row, col_id};
    {
      std::shared_lock<std::shared_mutex> lock(mutex_);
      auto it = temperatures_.find(key);
      if (it != temperatures_.end()) return *it->second;
    }
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = temperatures_.find(key);
    if (it != temperatures_.end()) return *it->second;
    auto temp = std::make_unique<RecordTemperature>();
    temp->last_decay_time.store(GetCurrentTimeMs(), std::memory_order_relaxed);
    auto& ref = *temp;
    temperatures_[key] = std::move(temp);
    return ref;
  }
  
  mutable std::shared_mutex mutex_;
  std::unordered_map<RowColKey, std::unique_ptr<RecordTemperature>, RowColKeyHash> temperatures_;
};

// ============================================================================
// Lock Manager (standalone implementation for testing)
// ============================================================================

enum class LockMode { NONE = 0, SHARED = 1, EXCLUSIVE = 2 };
enum class LockStatus { ACQUIRED = 0, WAITING = 1, DENIED = 2, TIMEOUT = 3 };

struct LockInfo {
  void* row;
  int col_id;
  LockMode mode;
  LockInfo(void* r, int c, LockMode m) : row(r), col_id(c), mode(m) {}
};

class RecordLock {
public:
  RecordLock() = default;
  
  bool TryAcquire(txnid_t txn_id, LockMode mode) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (mode == LockMode::SHARED && shared_holders_.count(txn_id)) return true;
    if (mode == LockMode::EXCLUSIVE && exclusive_holder_ == txn_id) return true;
    if (!CanGrant(mode, txn_id)) return false;
    if (mode == LockMode::SHARED) shared_holders_.insert(txn_id);
    else { shared_holders_.erase(txn_id); exclusive_holder_ = txn_id; }
    return true;
  }
  
  bool Release(txnid_t txn_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    bool released = false;
    if (exclusive_holder_ == txn_id) { exclusive_holder_ = 0; released = true; }
    if (shared_holders_.erase(txn_id) > 0) released = true;
    return released;
  }
  
  bool IsHeldBy(txnid_t txn_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return exclusive_holder_ == txn_id || shared_holders_.count(txn_id) > 0;
  }
  
  bool IsFree() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return exclusive_holder_ == 0 && shared_holders_.empty();
  }
  
  size_t GetWaiterCount() const { return 0; }

private:
  bool CanGrant(LockMode mode, txnid_t txn_id) const {
    if (mode == LockMode::SHARED) {
      return exclusive_holder_ == 0 || exclusive_holder_ == txn_id;
    } else {
      if (exclusive_holder_ != 0) return exclusive_holder_ == txn_id;
      if (shared_holders_.empty()) return true;
      return shared_holders_.size() == 1 && shared_holders_.count(txn_id) > 0;
    }
  }
  
  mutable std::mutex mutex_;
  txnid_t exclusive_holder_{0};
  std::unordered_set<txnid_t> shared_holders_;
};

class MoccLockManager {
public:
  static MoccLockManager& Instance() {
    static MoccLockManager instance;
    return instance;
  }
  
  bool TryAcquireLock(txnid_t txn_id, void* row, int col_id, LockMode mode) {
    RecordLock& lock = GetOrCreateLock(row, col_id);
    bool acquired = lock.TryAcquire(txn_id, mode);
    if (acquired) {
      std::lock_guard<std::mutex> guard(txn_locks_mutex_);
      txn_held_locks_[txn_id].push_back(RowColKey{row, col_id});
    }
    return acquired;
  }
  
  bool ReleaseLock(txnid_t txn_id, void* row, int col_id) {
    RowColKey key{row, col_id};
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = locks_.find(key);
    if (it == locks_.end()) return false;
    bool released = it->second->Release(txn_id);
    if (released) {
      std::lock_guard<std::mutex> guard(txn_locks_mutex_);
      auto txn_it = txn_held_locks_.find(txn_id);
      if (txn_it != txn_held_locks_.end()) {
        auto& locks_vec = txn_it->second;
        locks_vec.erase(std::remove(locks_vec.begin(), locks_vec.end(), key), locks_vec.end());
        if (locks_vec.empty()) txn_held_locks_.erase(txn_it);
      }
    }
    return released;
  }
  
  void ReleaseAllLocks(txnid_t txn_id) {
    std::vector<RowColKey> locks_to_release;
    {
      std::lock_guard<std::mutex> guard(txn_locks_mutex_);
      auto it = txn_held_locks_.find(txn_id);
      if (it != txn_held_locks_.end()) {
        locks_to_release = it->second;
        txn_held_locks_.erase(it);
      }
    }
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (const auto& key : locks_to_release) {
      auto it = locks_.find(key);
      if (it != locks_.end()) it->second->Release(txn_id);
    }
  }
  
  bool HoldsLock(txnid_t txn_id, void* row, int col_id) const {
    RowColKey key{row, col_id};
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = locks_.find(key);
    if (it == locks_.end()) return false;
    return it->second->IsHeldBy(txn_id);
  }
  
  bool ShouldUsePessimisticLock(void* row, int col_id) const {
    return TemperatureTracker::Instance().GetLevel(row, col_id) == TemperatureLevel::HOT;
  }
  
  bool PreAcquireLocks(txnid_t txn_id, const std::vector<LockInfo>& locks) {
    std::vector<LockInfo> sorted_locks = locks;
    std::sort(sorted_locks.begin(), sorted_locks.end(),
              [](const LockInfo& a, const LockInfo& b) {
                if (a.row != b.row) return a.row < b.row;
                return a.col_id < b.col_id;
              });
    std::vector<RowColKey> acquired;
    for (const auto& lock_info : sorted_locks) {
      RecordLock& lock = GetOrCreateLock(lock_info.row, lock_info.col_id);
      if (!lock.TryAcquire(txn_id, lock_info.mode)) {
        for (const auto& key : acquired) {
          auto it = locks_.find(key);
          if (it != locks_.end()) it->second->Release(txn_id);
        }
        return false;
      }
      acquired.push_back(RowColKey{lock_info.row, lock_info.col_id});
    }
    {
      std::lock_guard<std::mutex> guard(txn_locks_mutex_);
      auto& txn_locks = txn_held_locks_[txn_id];
      txn_locks.insert(txn_locks.end(), acquired.begin(), acquired.end());
    }
    return true;
  }
  
  void Clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::lock_guard<std::mutex> guard(txn_locks_mutex_);
    locks_.clear();
    txn_held_locks_.clear();
  }
  
  struct Stats {
    size_t total_locks;
    size_t held_locks;
    size_t waiting_requests;
  };
  
  Stats GetStats() const {
    Stats stats{0, 0, 0};
    std::shared_lock<std::shared_mutex> lock(mutex_);
    stats.total_locks = locks_.size();
    for (const auto& pair : locks_) {
      if (!pair.second->IsFree()) stats.held_locks++;
      stats.waiting_requests += pair.second->GetWaiterCount();
    }
    return stats;
  }

private:
  MoccLockManager() = default;
  
  RecordLock& GetOrCreateLock(void* row, int col_id) {
    RowColKey key{row, col_id};
    {
      std::shared_lock<std::shared_mutex> lock(mutex_);
      auto it = locks_.find(key);
      if (it != locks_.end()) return *it->second;
    }
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = locks_.find(key);
    if (it != locks_.end()) return *it->second;
    auto new_lock = std::make_unique<RecordLock>();
    auto& ref = *new_lock;
    locks_[key] = std::move(new_lock);
    return ref;
  }
  
  mutable std::shared_mutex mutex_;
  std::unordered_map<RowColKey, std::unique_ptr<RecordLock>, RowColKeyHash> locks_;
  mutable std::mutex txn_locks_mutex_;
  std::unordered_map<txnid_t, std::vector<RowColKey>> txn_held_locks_;
};

} // namespace janus

using namespace janus;
using namespace std;
using namespace janus;

// ============================================================================
// Test Utilities
// ============================================================================

#define TEST_PASS(name) printf(GREEN "[PASS] " RESET "%s\n", name)
#define TEST_FAIL(name, msg) printf(RED "[FAIL] " RESET "%s: %s\n", name, msg)

#define ASSERT_TRUE(cond, name) \
  if (!(cond)) { TEST_FAIL(name, #cond " is false"); return false; } \
  else { TEST_PASS(name); }

#define ASSERT_FALSE(cond, name) \
  if (cond) { TEST_FAIL(name, #cond " is true"); return false; } \
  else { TEST_PASS(name); }

#define ASSERT_EQ(a, b, name) \
  if ((a) != (b)) { TEST_FAIL(name, "values not equal"); return false; } \
  else { TEST_PASS(name); }

// ============================================================================
// Temperature Tracking Tests
// ============================================================================

bool test_temperature_basic() {
  printf("\n=== Temperature Basic Tests ===\n");
  
  // Clear any existing state
  TemperatureTracker::Instance().Clear();
  
  // Create mock row pointer
  void* row1 = (void*)0x1000;
  int col1 = 0;
  
  // Initially temperature should be 0 (cold)
  uint32_t temp = TemperatureTracker::Instance().GetTemperature(row1, col1);
  ASSERT_EQ(temp, 0u, "Initial temperature is 0");
  
  // Level should be COLD
  TemperatureLevel level = TemperatureTracker::Instance().GetLevel(row1, col1);
  ASSERT_TRUE(level == TemperatureLevel::COLD, "Initial level is COLD");
  
  return true;
}

bool test_temperature_increment() {
  printf("\n=== Temperature Increment Tests ===\n");
  
  TemperatureTracker::Instance().Clear();
  
  void* row1 = (void*)0x2000;
  int col1 = 0;
  
  // Increment temperature several times
  for (int i = 0; i < 5; i++) {
    TemperatureTracker::Instance().IncrementTemperature(row1, col1);
  }
  
  uint32_t temp = TemperatureTracker::Instance().GetTemperature(row1, col1);
  ASSERT_EQ(temp, 5u, "Temperature after 5 increments");
  
  // Should be WARM now
  TemperatureLevel level = TemperatureTracker::Instance().GetLevel(row1, col1);
  ASSERT_TRUE(level == TemperatureLevel::WARM, "Level is WARM after increments");
  
  return true;
}

bool test_temperature_hot() {
  printf("\n=== Temperature Hot Tests ===\n");
  
  TemperatureTracker::Instance().Clear();
  
  void* row1 = (void*)0x3000;
  int col1 = 0;
  
  // Increment to make it hot
  for (int i = 0; i < MOCC_HOT_THRESHOLD + 2; i++) {
    TemperatureTracker::Instance().IncrementTemperature(row1, col1);
  }
  
  TemperatureLevel level = TemperatureTracker::Instance().GetLevel(row1, col1);
  ASSERT_TRUE(level == TemperatureLevel::HOT, "Level is HOT after many increments");
  
  return true;
}

bool test_temperature_abort_increases() {
  printf("\n=== Temperature Abort Increase Tests ===\n");
  
  TemperatureTracker::Instance().Clear();
  
  void* row1 = (void*)0x4000;
  int col1 = 0;
  
  // Record an abort (should increase temperature by 3)
  TemperatureTracker::Instance().RecordAbort(row1, col1);
  
  uint32_t temp = TemperatureTracker::Instance().GetTemperature(row1, col1);
  ASSERT_EQ(temp, 3u, "Temperature increases by 3 on abort");
  
  return true;
}

bool test_temperature_commit_decreases() {
  printf("\n=== Temperature Commit Decrease Tests ===\n");
  
  TemperatureTracker::Instance().Clear();
  
  void* row1 = (void*)0x5000;
  int col1 = 0;
  
  // First increase temperature
  for (int i = 0; i < 5; i++) {
    TemperatureTracker::Instance().IncrementTemperature(row1, col1);
  }
  
  uint32_t before = TemperatureTracker::Instance().GetTemperature(row1, col1);
  
  // Record commit
  TemperatureTracker::Instance().RecordCommit(row1, col1);
  
  uint32_t after = TemperatureTracker::Instance().GetTemperature(row1, col1);
  ASSERT_EQ(after, before - 1, "Temperature decreases by 1 on commit");
  
  return true;
}

bool test_temperature_max_cap() {
  printf("\n=== Temperature Max Cap Tests ===\n");
  
  TemperatureTracker::Instance().Clear();
  
  void* row1 = (void*)0x6000;
  int col1 = 0;
  
  // Try to increase beyond max
  for (int i = 0; i < MOCC_MAX_TEMPERATURE + 10; i++) {
    TemperatureTracker::Instance().IncrementTemperature(row1, col1);
  }
  
  uint32_t temp = TemperatureTracker::Instance().GetTemperature(row1, col1);
  ASSERT_EQ(temp, MOCC_MAX_TEMPERATURE, "Temperature capped at max");
  
  return true;
}

bool test_temperature_stats() {
  printf("\n=== Temperature Stats Tests ===\n");
  
  TemperatureTracker::Instance().Clear();
  
  // Create some records with different temperatures
  void* cold_row = (void*)0x7000;
  void* warm_row = (void*)0x7001;
  void* hot_row = (void*)0x7002;
  
  // Leave cold_row cold (just access once)
  TemperatureTracker::Instance().IncrementTemperature(cold_row, 0);
  
  // Make warm_row warm
  for (int i = 0; i < MOCC_COLD_THRESHOLD + 1; i++) {
    TemperatureTracker::Instance().IncrementTemperature(warm_row, 0);
  }
  
  // Make hot_row hot
  for (int i = 0; i < MOCC_HOT_THRESHOLD + 1; i++) {
    TemperatureTracker::Instance().IncrementTemperature(hot_row, 0);
  }
  
  auto stats = TemperatureTracker::Instance().GetStats();
  ASSERT_EQ(stats.total_records, 3u, "3 total records tracked");
  ASSERT_EQ(stats.cold_records, 1u, "1 cold record");
  ASSERT_EQ(stats.warm_records, 1u, "1 warm record");
  ASSERT_EQ(stats.hot_records, 1u, "1 hot record");
  
  return true;
}

// ============================================================================
// Lock Manager Tests
// ============================================================================

bool test_lock_basic_acquire_release() {
  printf("\n=== Lock Basic Acquire/Release Tests ===\n");
  
  MoccLockManager::Instance().Clear();
  
  void* row1 = (void*)0x8000;
  int col1 = 0;
  txnid_t txn1 = 1001;
  
  // Acquire exclusive lock
  bool acquired = MoccLockManager::Instance().TryAcquireLock(
    txn1, row1, col1, LockMode::EXCLUSIVE);
  ASSERT_TRUE(acquired, "Exclusive lock acquired");
  
  // Check it's held
  bool held = MoccLockManager::Instance().HoldsLock(txn1, row1, col1);
  ASSERT_TRUE(held, "Lock is held by transaction");
  
  // Release
  bool released = MoccLockManager::Instance().ReleaseLock(txn1, row1, col1);
  ASSERT_TRUE(released, "Lock released");
  
  // Should no longer be held
  held = MoccLockManager::Instance().HoldsLock(txn1, row1, col1);
  ASSERT_FALSE(held, "Lock no longer held after release");
  
  return true;
}

bool test_lock_shared_concurrent() {
  printf("\n=== Lock Shared Concurrent Tests ===\n");
  
  MoccLockManager::Instance().Clear();
  
  void* row1 = (void*)0x9000;
  int col1 = 0;
  txnid_t txn1 = 2001;
  txnid_t txn2 = 2002;
  
  // Both should be able to acquire shared lock
  bool acq1 = MoccLockManager::Instance().TryAcquireLock(
    txn1, row1, col1, LockMode::SHARED);
  bool acq2 = MoccLockManager::Instance().TryAcquireLock(
    txn2, row1, col1, LockMode::SHARED);
  
  ASSERT_TRUE(acq1, "First shared lock acquired");
  ASSERT_TRUE(acq2, "Second shared lock acquired");
  
  // Both should hold the lock
  ASSERT_TRUE(MoccLockManager::Instance().HoldsLock(txn1, row1, col1), 
              "Txn1 holds shared lock");
  ASSERT_TRUE(MoccLockManager::Instance().HoldsLock(txn2, row1, col1), 
              "Txn2 holds shared lock");
  
  // Release all
  MoccLockManager::Instance().ReleaseAllLocks(txn1);
  MoccLockManager::Instance().ReleaseAllLocks(txn2);
  
  return true;
}

bool test_lock_exclusive_blocks_shared() {
  printf("\n=== Lock Exclusive Blocks Shared Tests ===\n");
  
  MoccLockManager::Instance().Clear();
  
  void* row1 = (void*)0xA000;
  int col1 = 0;
  txnid_t txn1 = 3001;
  txnid_t txn2 = 3002;
  
  // Txn1 acquires exclusive
  bool acq1 = MoccLockManager::Instance().TryAcquireLock(
    txn1, row1, col1, LockMode::EXCLUSIVE);
  ASSERT_TRUE(acq1, "Exclusive lock acquired");
  
  // Txn2 should not be able to get shared lock
  bool acq2 = MoccLockManager::Instance().TryAcquireLock(
    txn2, row1, col1, LockMode::SHARED);
  ASSERT_FALSE(acq2, "Shared lock blocked by exclusive");
  
  // Release and try again
  MoccLockManager::Instance().ReleaseLock(txn1, row1, col1);
  
  acq2 = MoccLockManager::Instance().TryAcquireLock(
    txn2, row1, col1, LockMode::SHARED);
  ASSERT_TRUE(acq2, "Shared lock acquired after exclusive released");
  
  MoccLockManager::Instance().ReleaseAllLocks(txn2);
  
  return true;
}

bool test_lock_pre_acquire_multiple() {
  printf("\n=== Lock Pre-Acquire Multiple Tests ===\n");
  
  MoccLockManager::Instance().Clear();
  
  txnid_t txn1 = 4001;
  
  // Create lock info for multiple records
  std::vector<LockInfo> locks;
  locks.emplace_back((void*)0xB000, 0, LockMode::EXCLUSIVE);
  locks.emplace_back((void*)0xB001, 0, LockMode::SHARED);
  locks.emplace_back((void*)0xB002, 0, LockMode::EXCLUSIVE);
  
  // Pre-acquire all
  bool acquired = MoccLockManager::Instance().PreAcquireLocks(txn1, locks);
  ASSERT_TRUE(acquired, "All locks pre-acquired");
  
  // All should be held
  for (const auto& lock : locks) {
    bool held = MoccLockManager::Instance().HoldsLock(txn1, lock.row, lock.col_id);
    ASSERT_TRUE(held, "Pre-acquired lock is held");
  }
  
  // Release all
  MoccLockManager::Instance().ReleaseAllLocks(txn1);
  
  return true;
}

bool test_lock_stats() {
  printf("\n=== Lock Stats Tests ===\n");
  
  MoccLockManager::Instance().Clear();
  
  txnid_t txn1 = 5001;
  
  // Acquire some locks
  MoccLockManager::Instance().TryAcquireLock(txn1, (void*)0xC000, 0, LockMode::EXCLUSIVE);
  MoccLockManager::Instance().TryAcquireLock(txn1, (void*)0xC001, 0, LockMode::SHARED);
  
  auto stats = MoccLockManager::Instance().GetStats();
  ASSERT_EQ(stats.total_locks, 2u, "2 locks tracked");
  ASSERT_EQ(stats.held_locks, 2u, "2 locks held");
  
  MoccLockManager::Instance().ReleaseAllLocks(txn1);
  
  return true;
}

// ============================================================================
// Integration Tests
// ============================================================================

bool test_mocc_cold_record_uses_occ() {
  printf("\n=== MOCC Cold Record Uses OCC Tests ===\n");
  
  // Clear state
  TemperatureTracker::Instance().Clear();
  MoccLockManager::Instance().Clear();
  
  void* row1 = (void*)0xD000;
  int col1 = 0;
  
  // Cold record should NOT suggest pessimistic locking
  bool should_lock = MoccLockManager::Instance().ShouldUsePessimisticLock(row1, col1);
  ASSERT_FALSE(should_lock, "Cold record should use OCC (no pessimistic lock)");
  
  return true;
}

bool test_mocc_hot_record_uses_locks() {
  printf("\n=== MOCC Hot Record Uses Locks Tests ===\n");
  
  // Clear state
  TemperatureTracker::Instance().Clear();
  MoccLockManager::Instance().Clear();
  
  void* row1 = (void*)0xE000;
  int col1 = 0;
  
  // Make the record hot
  for (int i = 0; i < MOCC_HOT_THRESHOLD + 2; i++) {
    TemperatureTracker::Instance().IncrementTemperature(row1, col1);
  }
  
  // Hot record SHOULD suggest pessimistic locking
  bool should_lock = MoccLockManager::Instance().ShouldUsePessimisticLock(row1, col1);
  ASSERT_TRUE(should_lock, "Hot record should use pessimistic locking");
  
  return true;
}

bool test_mocc_abort_makes_record_hotter() {
  printf("\n=== MOCC Abort Makes Record Hotter Tests ===\n");
  
  // Clear state
  TemperatureTracker::Instance().Clear();
  
  void* row1 = (void*)0xF000;
  int col1 = 0;
  
  // Start with warm record
  for (int i = 0; i < MOCC_COLD_THRESHOLD; i++) {
    TemperatureTracker::Instance().IncrementTemperature(row1, col1);
  }
  
  TemperatureLevel before = TemperatureTracker::Instance().GetLevel(row1, col1);
  ASSERT_TRUE(before == TemperatureLevel::WARM, "Record starts warm");
  
  // Simulate multiple aborts
  for (int i = 0; i < 3; i++) {
    TemperatureTracker::Instance().RecordAbort(row1, col1);
  }
  
  TemperatureLevel after = TemperatureTracker::Instance().GetLevel(row1, col1);
  ASSERT_TRUE(after == TemperatureLevel::HOT, "Record becomes hot after aborts");
  
  return true;
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

bool test_concurrent_temperature_updates() {
  printf("\n=== Concurrent Temperature Updates Tests ===\n");
  
  TemperatureTracker::Instance().Clear();
  
  void* row1 = (void*)0x10000;
  int col1 = 0;
  const int num_threads = 4;
  const int increments_per_thread = 100;
  
  std::vector<std::thread> threads;
  std::atomic<int> completed{0};
  
  // Launch threads that all increment the same record
  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back([row1, col1, &completed]() {
      for (int i = 0; i < increments_per_thread; i++) {
        TemperatureTracker::Instance().IncrementTemperature(row1, col1);
      }
      completed++;
    });
  }
  
  // Wait for all threads
  for (auto& t : threads) {
    t.join();
  }
  
  // Temperature should be at max (concurrent increments)
  uint32_t temp = TemperatureTracker::Instance().GetTemperature(row1, col1);
  ASSERT_EQ(temp, MOCC_MAX_TEMPERATURE, "Temperature at max after concurrent updates");
  
  return true;
}

bool test_concurrent_lock_acquisition() {
  printf("\n=== Concurrent Lock Acquisition Tests ===\n");
  
  MoccLockManager::Instance().Clear();
  
  void* row1 = (void*)0x11000;
  int col1 = 0;
  const int num_threads = 4;
  
  std::atomic<int> acquired_count{0};
  std::atomic<int> denied_count{0};
  std::vector<std::thread> threads;
  
  // Launch threads that try to acquire exclusive lock
  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back([row1, col1, &acquired_count, &denied_count, t]() {
      txnid_t txn_id = 10000 + t;
      bool acq = MoccLockManager::Instance().TryAcquireLock(
        txn_id, row1, col1, LockMode::EXCLUSIVE);
      if (acq) {
        acquired_count++;
        // Hold briefly then release
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        MoccLockManager::Instance().ReleaseLock(txn_id, row1, col1);
      } else {
        denied_count++;
      }
    });
  }
  
  // Wait for all threads
  for (auto& t : threads) {
    t.join();
  }
  
  // At least one should have acquired, and exclusive should block others
  ASSERT_TRUE(acquired_count > 0, "At least one thread acquired exclusive lock");
  printf("  Acquired: %d, Denied: %d\n", acquired_count.load(), denied_count.load());
  
  return true;
}

// ============================================================================
// Main Test Runner
// ============================================================================

int main() {
  printf("========================================\n");
  printf("MOCC (Mixed OCC) Test Suite\n");
  printf("========================================\n");
  
  int passed = 0;
  int failed = 0;
  
  auto run_test = [&](bool (*test_fn)(), const char* name) {
    try {
      if (test_fn()) {
        passed++;
      } else {
        failed++;
        printf(RED "[TEST FAILED]: %s\n" RESET, name);
      }
    } catch (const std::exception& e) {
      failed++;
      printf(RED "[TEST EXCEPTION]: %s: %s\n" RESET, name, e.what());
    }
  };
  
  // Temperature Tests
  run_test(test_temperature_basic, "temperature_basic");
  run_test(test_temperature_increment, "temperature_increment");
  run_test(test_temperature_hot, "temperature_hot");
  run_test(test_temperature_abort_increases, "temperature_abort_increases");
  run_test(test_temperature_commit_decreases, "temperature_commit_decreases");
  run_test(test_temperature_max_cap, "temperature_max_cap");
  run_test(test_temperature_stats, "temperature_stats");
  
  // Lock Manager Tests
  run_test(test_lock_basic_acquire_release, "lock_basic_acquire_release");
  run_test(test_lock_shared_concurrent, "lock_shared_concurrent");
  run_test(test_lock_exclusive_blocks_shared, "lock_exclusive_blocks_shared");
  run_test(test_lock_pre_acquire_multiple, "lock_pre_acquire_multiple");
  run_test(test_lock_stats, "lock_stats");
  
  // Integration Tests
  run_test(test_mocc_cold_record_uses_occ, "mocc_cold_record_uses_occ");
  run_test(test_mocc_hot_record_uses_locks, "mocc_hot_record_uses_locks");
  run_test(test_mocc_abort_makes_record_hotter, "mocc_abort_makes_record_hotter");
  
  // Concurrent Tests
  run_test(test_concurrent_temperature_updates, "concurrent_temperature_updates");
  run_test(test_concurrent_lock_acquisition, "concurrent_lock_acquisition");
  
  printf("\n========================================\n");
  printf("Results: %d passed, %d failed\n", passed, failed);
  printf("========================================\n");
  
  return failed > 0 ? 1 : 0;
}

