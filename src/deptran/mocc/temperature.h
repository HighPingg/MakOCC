#pragma once

/**
 * MOCC Temperature Tracking System
 * 
 * This module implements the temperature tracking mechanism for MOCC
 * (Mixed Optimistic/Pessimistic Concurrency Control).
 * 
 * Temperature represents the "hotness" of a record - how frequently it's
 * accessed and causes conflicts. Hot records (high temperature) benefit
 * from pessimistic locking, while cold records use optimistic concurrency.
 * 
 * Reference: "MOCC: Optimistic Concurrency Control Based on Monitoring
 * for Main Memory Databases" (ICDE 2016)
 */

#include "../__dep__.h"
#include <atomic>
#include <unordered_map>
#include <shared_mutex>

namespace janus {

// Temperature thresholds for switching between OCC and pessimistic locking
constexpr uint32_t MOCC_COLD_THRESHOLD = 3;      // Below this = cold (use OCC)
constexpr uint32_t MOCC_HOT_THRESHOLD = 10;      // Above this = hot (use locks)
constexpr uint32_t MOCC_MAX_TEMPERATURE = 20;    // Temperature cap

// Temperature decay interval (in milliseconds)
constexpr uint64_t MOCC_DECAY_INTERVAL_MS = 100;

// Temperature decay factor (temperature *= DECAY_FACTOR each interval)
constexpr double MOCC_DECAY_FACTOR = 0.9;

/**
 * TemperatureLevel indicates how a record should be accessed
 */
enum class TemperatureLevel {
  COLD = 0,     // Use optimistic concurrency control
  WARM = 1,     // Transitioning - use OCC but monitor
  HOT = 2       // Use pessimistic locking
};

/**
 * RecordTemperature tracks temperature for a single record/row
 */
struct RecordTemperature {
  std::atomic<uint32_t> temperature{0};
  std::atomic<uint32_t> abort_count{0};
  std::atomic<uint64_t> last_access_time{0};
  std::atomic<uint64_t> last_decay_time{0};
  
  RecordTemperature() = default;
  
  // Disable copy
  RecordTemperature(const RecordTemperature&) = delete;
  RecordTemperature& operator=(const RecordTemperature&) = delete;
  
  // Allow move
  RecordTemperature(RecordTemperature&& other) noexcept
    : temperature(other.temperature.load()),
      abort_count(other.abort_count.load()),
      last_access_time(other.last_access_time.load()),
      last_decay_time(other.last_decay_time.load()) {}
};

/**
 * Row-Column pair for identifying specific cells in the database
 */
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

/**
 * TemperatureTracker is a singleton that manages temperature for all records
 * across the system. It provides thread-safe access to temperature data.
 */
class TemperatureTracker {
public:
  static TemperatureTracker& Instance() {
    static TemperatureTracker instance;
    return instance;
  }
  
  /**
   * Get the temperature level for a specific row/column
   * @param row Pointer to the row
   * @param col_id Column ID (-1 for entire row)
   * @return TemperatureLevel indicating how to access the record
   */
  TemperatureLevel GetLevel(void* row, int col_id = -1);
  
  /**
   * Get raw temperature value for a record
   */
  uint32_t GetTemperature(void* row, int col_id = -1);
  
  /**
   * Increment temperature when a record is accessed
   * @param row Pointer to the row
   * @param col_id Column ID (-1 for entire row)
   * @param increment Amount to increase (default 1)
   */
  void IncrementTemperature(void* row, int col_id = -1, uint32_t increment = 1);
  
  /**
   * Record an abort on this record (increases temperature more)
   * @param row Pointer to the row
   * @param col_id Column ID (-1 for entire row)
   */
  void RecordAbort(void* row, int col_id = -1);
  
  /**
   * Record a successful commit (slightly decreases temperature)
   * @param row Pointer to the row
   * @param col_id Column ID (-1 for entire row)
   */
  void RecordCommit(void* row, int col_id = -1);
  
  /**
   * Apply temperature decay to all records
   * Should be called periodically
   */
  void ApplyDecay();
  
  /**
   * Reset temperature for a specific record
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
    size_t total_records;
    size_t hot_records;
    size_t warm_records;
    size_t cold_records;
    uint64_t total_temperature;
  };
  Stats GetStats();

private:
  TemperatureTracker() = default;
  ~TemperatureTracker() = default;
  
  // Disable copy/move
  TemperatureTracker(const TemperatureTracker&) = delete;
  TemperatureTracker& operator=(const TemperatureTracker&) = delete;
  
  // Get or create temperature entry for a record
  RecordTemperature& GetOrCreate(void* row, int col_id);
  
  // Apply decay to a single record
  void ApplyDecayToRecord(RecordTemperature& temp);
  
  // Storage for temperature data
  // Using shared_mutex for read-heavy workloads
  mutable std::shared_mutex mutex_;
  std::unordered_map<RowColKey, std::unique_ptr<RecordTemperature>, RowColKeyHash> temperatures_;
  
  // Timestamp for decay
  std::atomic<uint64_t> last_global_decay_time_{0};
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




