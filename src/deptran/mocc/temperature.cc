/**
 * MOCC Temperature Tracking Implementation
 */

#include "temperature.h"

namespace janus {

TemperatureLevel TemperatureTracker::GetLevel(void* row, int col_id) {
  uint32_t temp = GetTemperature(row, col_id);
  
  if (temp >= MOCC_HOT_THRESHOLD) {
    return TemperatureLevel::HOT;
  } else if (temp >= MOCC_COLD_THRESHOLD) {
    return TemperatureLevel::WARM;
  } else {
    return TemperatureLevel::COLD;
  }
}

uint32_t TemperatureTracker::GetTemperature(void* row, int col_id) {
  RowColKey key{row, col_id};
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = temperatures_.find(key);
  if (it == temperatures_.end()) {
    return 0;
  }
  return it->second->temperature.load(std::memory_order_relaxed);
}

void TemperatureTracker::IncrementTemperature(void* row, int col_id, uint32_t increment) {
  RecordTemperature& temp = GetOrCreate(row, col_id);
  
  // Atomic increment with cap
  uint32_t current = temp.temperature.load(std::memory_order_relaxed);
  uint32_t new_temp;
  do {
    new_temp = std::min(current + increment, MOCC_MAX_TEMPERATURE);
  } while (!temp.temperature.compare_exchange_weak(current, new_temp,
                                                    std::memory_order_relaxed));
  
  // Update last access time
  temp.last_access_time.store(GetCurrentTimeMs(), std::memory_order_relaxed);
}

void TemperatureTracker::RecordAbort(void* row, int col_id) {
  RecordTemperature& temp = GetOrCreate(row, col_id);
  
  // Increment abort count
  temp.abort_count.fetch_add(1, std::memory_order_relaxed);
  
  // Increase temperature more significantly on abort (by 3)
  uint32_t current = temp.temperature.load(std::memory_order_relaxed);
  uint32_t new_temp;
  do {
    new_temp = std::min(current + 3, MOCC_MAX_TEMPERATURE);
  } while (!temp.temperature.compare_exchange_weak(current, new_temp,
                                                    std::memory_order_relaxed));
  
  temp.last_access_time.store(GetCurrentTimeMs(), std::memory_order_relaxed);
}

void TemperatureTracker::RecordCommit(void* row, int col_id) {
  RowColKey key{row, col_id};
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = temperatures_.find(key);
  if (it == temperatures_.end()) {
    return;  // No temperature entry, nothing to update
  }
  
  RecordTemperature& temp = *it->second;
  
  // Slightly decrease temperature on commit (encourages optimism)
  uint32_t current = temp.temperature.load(std::memory_order_relaxed);
  if (current > 0) {
    temp.temperature.compare_exchange_weak(current, current - 1,
                                           std::memory_order_relaxed);
  }
}

void TemperatureTracker::ApplyDecay() {
  uint64_t now = GetCurrentTimeMs();
  uint64_t last_decay = last_global_decay_time_.load(std::memory_order_relaxed);
  
  // Only decay if enough time has passed
  if (now - last_decay < MOCC_DECAY_INTERVAL_MS) {
    return;
  }
  
  // Try to claim the decay operation
  if (!last_global_decay_time_.compare_exchange_strong(last_decay, now,
                                                        std::memory_order_relaxed)) {
    return;  // Another thread is doing decay
  }
  
  // Apply decay to all records
  std::shared_lock<std::shared_mutex> lock(mutex_);
  for (auto& pair : temperatures_) {
    ApplyDecayToRecord(*pair.second);
  }
}

void TemperatureTracker::ApplyDecayToRecord(RecordTemperature& temp) {
  uint64_t now = GetCurrentTimeMs();
  uint64_t last_decay = temp.last_decay_time.load(std::memory_order_relaxed);
  
  if (now - last_decay < MOCC_DECAY_INTERVAL_MS) {
    return;
  }
  
  // Apply exponential decay
  uint32_t current = temp.temperature.load(std::memory_order_relaxed);
  if (current > 0) {
    uint32_t new_temp = static_cast<uint32_t>(current * MOCC_DECAY_FACTOR);
    temp.temperature.store(new_temp, std::memory_order_relaxed);
  }
  
  temp.last_decay_time.store(now, std::memory_order_relaxed);
}

void TemperatureTracker::ResetTemperature(void* row, int col_id) {
  RowColKey key{row, col_id};
  
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = temperatures_.find(key);
  if (it != temperatures_.end()) {
    it->second->temperature.store(0, std::memory_order_relaxed);
    it->second->abort_count.store(0, std::memory_order_relaxed);
  }
}

void TemperatureTracker::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  temperatures_.clear();
}

RecordTemperature& TemperatureTracker::GetOrCreate(void* row, int col_id) {
  RowColKey key{row, col_id};
  
  // First try with shared lock (read)
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = temperatures_.find(key);
    if (it != temperatures_.end()) {
      return *it->second;
    }
  }
  
  // Need to create - use exclusive lock
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  // Double-check after acquiring exclusive lock
  auto it = temperatures_.find(key);
  if (it != temperatures_.end()) {
    return *it->second;
  }
  
  // Create new entry
  auto temp = std::make_unique<RecordTemperature>();
  temp->last_decay_time.store(GetCurrentTimeMs(), std::memory_order_relaxed);
  auto& ref = *temp;
  temperatures_[key] = std::move(temp);
  return ref;
}

TemperatureTracker::Stats TemperatureTracker::GetStats() {
  Stats stats{0, 0, 0, 0, 0};
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  for (const auto& pair : temperatures_) {
    stats.total_records++;
    uint32_t temp = pair.second->temperature.load(std::memory_order_relaxed);
    stats.total_temperature += temp;
    
    if (temp >= MOCC_HOT_THRESHOLD) {
      stats.hot_records++;
    } else if (temp >= MOCC_COLD_THRESHOLD) {
      stats.warm_records++;
    } else {
      stats.cold_records++;
    }
  }
  
  return stats;
}

} // namespace janus




