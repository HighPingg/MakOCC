/**
 * MOCC Temperature Tracking Implementation
 * 
 * Implements MOCC paper Section 3.2:
 * - Approximate counters to reduce cacheline invalidation
 * - Temperature only increases on abort
 * - Periodic reset for workload adaptation
 */

#include "temperature.h"
#include <cmath>

namespace janus {

// Thread-local random number generator for approximate counter
thread_local std::mt19937 TemperatureTracker::rng_(std::random_device{}());
thread_local std::uniform_real_distribution<double> TemperatureTracker::dist_(0.0, 1.0);

TemperatureTracker::TemperatureTracker() {
  last_global_reset_time_.store(GetCurrentTimeMs(), std::memory_order_relaxed);
}

void* TemperatureTracker::GetPageAddress(void* row) const {
  // Align to page boundary (4KB pages)
  uintptr_t addr = reinterpret_cast<uintptr_t>(row);
  return reinterpret_cast<void*>(addr & ~(PAGE_SIZE - 1));
}

TemperatureLevel TemperatureTracker::GetLevel(void* row, int col_id) {
  uint32_t temp = GetTemperature(row, col_id);
  
  if (temp >= MOCC_HOT_THRESHOLD) {
    return TemperatureLevel::HOT;
  } else if (temp >= MOCC_WARM_THRESHOLD) {
    return TemperatureLevel::WARM;
  } else {
    return TemperatureLevel::COLD;
  }
}

uint32_t TemperatureTracker::GetTemperature(void* row, int col_id) {
  void* page_addr = GetPageAddress(row);
  PageKey key{page_addr};
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = temperatures_.find(key);
  if (it == temperatures_.end()) {
    return 0;
  }
  return it->second->temperature.load(std::memory_order_relaxed);
}

bool TemperatureTracker::ShouldIncrement(uint32_t current_temp) {
  // MOCC Algorithm 2: Approximate counter
  // Increment with probability 2^(-temp)
  // This reduces cacheline invalidation on hot pages
  
  if (current_temp == 0) {
    return true;  // Always increment from 0
  }
  
  if (current_temp >= MOCC_MAX_TEMPERATURE) {
    return false;  // At cap, don't increment
  }
  
  // Probability = 2^(-temp)
  double probability = std::pow(2.0, -static_cast<double>(current_temp));
  return dist_(rng_) < probability;
}

void TemperatureTracker::RecordAbort(void* row, int col_id) {
  // MOCC paper: Temperature ONLY increases on abort
  void* page_addr = GetPageAddress(row);
  PageTemperature& temp = GetOrCreate(page_addr);
  
  uint32_t current = temp.temperature.load(std::memory_order_relaxed);
  
  // Use approximate counter: increment with probability 2^(-temp)
  if (ShouldIncrement(current)) {
    uint32_t new_temp;
    do {
      new_temp = std::min(current + 1, MOCC_MAX_TEMPERATURE);
    } while (!temp.temperature.compare_exchange_weak(current, new_temp,
                                                      std::memory_order_relaxed));
  }
}

void TemperatureTracker::RecordCommit(void* row, int col_id) {
  // MOCC paper: Commits do NOT decrease temperature
  // Temperature only decreases via periodic reset
  // This method is kept for API compatibility but does nothing
  (void)row;
  (void)col_id;
}

void TemperatureTracker::IncrementTemperature(void* row, int col_id, uint32_t increment) {
  // NOTE: In MOCC, temperature should ONLY increase on abort.
  // This method is kept for backward compatibility but will be deprecated.
  // Consider calling RecordAbort() instead.
  
  // For now, we'll allow direct increment but cap it
  void* page_addr = GetPageAddress(row);
  PageTemperature& temp = GetOrCreate(page_addr);
  
  uint32_t current = temp.temperature.load(std::memory_order_relaxed);
  uint32_t new_temp;
  do {
    new_temp = std::min(current + increment, MOCC_MAX_TEMPERATURE);
  } while (!temp.temperature.compare_exchange_weak(current, new_temp,
                                                    std::memory_order_relaxed));
}

void TemperatureTracker::ApplyDecay() {
  uint64_t now = GetCurrentTimeMs();
  uint64_t last_reset = last_global_reset_time_.load(std::memory_order_relaxed);
  
  // Only reset if enough time has passed
  if (now - last_reset < MOCC_DECAY_INTERVAL_MS) {
    return;
  }
  
  // Try to claim the reset operation
  if (!last_global_reset_time_.compare_exchange_strong(last_reset, now,
                                                        std::memory_order_relaxed)) {
    return;  // Another thread is doing reset
  }
  
  // MOCC paper: Periodically reset temperatures to zero
  // This allows the system to adapt to workload changes
  std::unique_lock<std::shared_mutex> lock(mutex_);
  for (auto& pair : temperatures_) {
    pair.second->temperature.store(0, std::memory_order_relaxed);
    pair.second->last_reset_time.store(now, std::memory_order_relaxed);
  }
}

void TemperatureTracker::ResetTemperature(void* row, int col_id) {
  void* page_addr = GetPageAddress(row);
  PageKey key{page_addr};
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = temperatures_.find(key);
  if (it != temperatures_.end()) {
    it->second->temperature.store(0, std::memory_order_relaxed);
  }
}

void TemperatureTracker::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  temperatures_.clear();
  last_global_reset_time_.store(GetCurrentTimeMs(), std::memory_order_relaxed);
}

PageTemperature& TemperatureTracker::GetOrCreate(void* page_addr) {
  PageKey key{page_addr};
  
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
  auto temp = std::make_unique<PageTemperature>();
  temp->last_reset_time.store(GetCurrentTimeMs(), std::memory_order_relaxed);
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
    } else if (temp >= MOCC_WARM_THRESHOLD) {
      stats.warm_records++;
    } else {
      stats.cold_records++;
    }
  }
  
  return stats;
}

} // namespace janus
