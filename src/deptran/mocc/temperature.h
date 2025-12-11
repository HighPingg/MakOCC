#pragma once

/**
 * MOCC Temperature Tracking System
 * 
 * This module implements the temperature tracking mechanism from the MOCC paper
 * (Wang & Kimura, VLDB 2016).
 * 
 * Key Features (per MOCC paper Section 3.2):
 * - Temperature tracked at PAGE granularity (not per-record) to reduce overhead
 * - Uses APPROXIMATE COUNTERS: increment with probability 2^(-temp)
 * - Temperature ONLY increases on ABORT (verification failure)
 * - Periodic decay to adapt to workload changes
 * 
 * Temperature represents the "hotness" of a record - how frequently it causes
 * conflicts. Hot records (high temperature) benefit from pessimistic locking,
 * while cold records use optimistic concurrency.
 */

#include "../__dep__.h"
#include <atomic>
#include <unordered_map>
#include <shared_mutex>
#include <random>
#include <chrono>

namespace janus {

// Temperature thresholds (MOCC paper recommends H=5-10, default 10)
constexpr uint32_t MOCC_HOT_THRESHOLD = 10;      // Above this = hot (use locks)
constexpr uint32_t MOCC_WARM_THRESHOLD = 5;      // Above this = warm
constexpr uint32_t MOCC_MAX_TEMPERATURE = 20;    // Temperature cap

// Temperature decay interval (in milliseconds)
constexpr uint64_t MOCC_DECAY_INTERVAL_MS = 1000;  // Reset periodically

/**
 * TemperatureLevel indicates how a record should be accessed
 */
enum class TemperatureLevel {
  COLD = 0,     // Use optimistic concurrency control
  WARM = 1,     // Transitioning - use OCC but monitor
  HOT = 2       // Use pessimistic locking
};

/**
 * PageTemperature tracks temperature for a single page
 * Uses approximate counter as per MOCC paper Algorithm 2
 */
struct PageTemperature {
  std::atomic<uint32_t> temperature{0};
  std::atomic<uint64_t> last_reset_time{0};
  
  PageTemperature() = default;
  
  // Disable copy
  PageTemperature(const PageTemperature&) = delete;
  PageTemperature& operator=(const PageTemperature&) = delete;
  
  // Allow move
  PageTemperature(PageTemperature&& other) noexcept
    : temperature(other.temperature.load()),
      last_reset_time(other.last_reset_time.load()) {}
};

/**
 * Page key for identifying pages (using row pointer's page address)
 * MOCC tracks temperature at page granularity
 */
struct PageKey {
  void* page_ptr;  // Page address (derived from row pointer)
  
  bool operator==(const PageKey& other) const {
    return page_ptr == other.page_ptr;
  }
};

struct PageKeyHash {
  size_t operator()(const PageKey& key) const {
    return std::hash<void*>()(key.page_ptr);
  }
};

/**
 * TemperatureTracker is a singleton that manages temperature for all pages
 * across the system. It provides thread-safe access to temperature data.
 * 
 * Implements MOCC paper Section 3.2:
 * - Approximate counters to reduce cacheline invalidation
 * - Temperature only increased on abort
 * - Periodic reset for workload adaptation
 */
class TemperatureTracker {
public:
  static TemperatureTracker& Instance() {
    static TemperatureTracker instance;
    return instance;
  }
  
  /**
   * Get the temperature level for a specific row
   * Looks up temperature at page granularity
   * 
   * @param row Pointer to the row
   * @param col_id Column ID (ignored - we track at page level)
   * @return TemperatureLevel indicating how to access the record
   */
  TemperatureLevel GetLevel(void* row, int col_id = -1);
  
  /**
   * Get raw temperature value for a page containing the row
   */
  uint32_t GetTemperature(void* row, int col_id = -1);
  
  /**
   * Record an abort on this record (increases temperature)
   * This is the PRIMARY way temperature increases in MOCC.
   * 
   * Uses approximate counter: increment with probability 2^(-temp)
   * This prevents hot pages from causing cacheline ping-pong.
   * 
   * @param row Pointer to the row
   * @param col_id Column ID (ignored - we track at page level)
   */
  void RecordAbort(void* row, int col_id = -1);
  
  /**
   * Record a successful commit
   * In MOCC, commits do NOT decrease temperature.
   * Temperature only decreases via periodic reset.
   * This method is kept for API compatibility but does minimal work.
   * 
   * @param row Pointer to the row
   * @param col_id Column ID (ignored)
   */
  void RecordCommit(void* row, int col_id = -1);
  
  /**
   * Increment temperature (internal use)
   * NOTE: In MOCC, temperature should only increase on abort.
   * This method is kept for backward compatibility but should
   * generally not be called directly.
   */
  void IncrementTemperature(void* row, int col_id = -1, uint32_t increment = 1);
  
  /**
   * Apply temperature reset/decay
   * MOCC periodically resets temperatures to adapt to workload changes.
   * Should be called periodically (e.g., every MOCC_DECAY_INTERVAL_MS).
   */
  void ApplyDecay();
  
  /**
   * Reset temperature for a specific page
   */
  void ResetTemperature(void* row, int col_id = -1);
  
  /**
   * Clear all temperature data
   */
  void Clear();
  
  /**
   * Get statistics for debugging/monitoring
   */
  struct Stats {
    size_t total_records;  // Total pages tracked
    size_t hot_records;    // Hot pages
    size_t warm_records;   // Warm pages
    size_t cold_records;   // Cold pages
    uint64_t total_temperature;
  };
  Stats GetStats();

private:
  TemperatureTracker();
  ~TemperatureTracker() = default;
  
  // Disable copy/move
  TemperatureTracker(const TemperatureTracker&) = delete;
  TemperatureTracker& operator=(const TemperatureTracker&) = delete;
  
  /**
   * Get page address from row pointer
   * Assumes standard page size alignment (4KB)
   */
  void* GetPageAddress(void* row) const;
  
  // Get or create temperature entry for a page
  PageTemperature& GetOrCreate(void* page_addr);
  
  /**
   * Approximate counter increment (MOCC Algorithm 2)
   * Returns true if we should increment, based on probability 2^(-temp)
   */
  bool ShouldIncrement(uint32_t current_temp);
  
  // Storage for temperature data (per page)
  mutable std::shared_mutex mutex_;
  std::unordered_map<PageKey, std::unique_ptr<PageTemperature>, PageKeyHash> temperatures_;
  
  // Timestamp for decay
  std::atomic<uint64_t> last_global_reset_time_{0};
  
  // Random number generator for approximate counter
  thread_local static std::mt19937 rng_;
  thread_local static std::uniform_real_distribution<double> dist_;
  
  // Page size for alignment (4KB default)
  static constexpr size_t PAGE_SIZE = 4096;
};

/**
 * Utility function to get current time in milliseconds
 */
inline uint64_t GetCurrentTimeMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now().time_since_epoch()
  ).count();
}

} // namespace janus
