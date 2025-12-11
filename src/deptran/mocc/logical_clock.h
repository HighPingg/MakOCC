#pragma once

/**
 * MOCC Logical Clock Infrastructure
 * 
 * This module implements the logical clock for MOCC's wait-die deadlock prevention.
 * 
 * Key Features:
 * - (clock, node_id) pair for total ordering across distributed nodes
 * - Lamport clock semantics for causal ordering
 * - Thread-safe atomic operations
 * 
 * The logical timestamp ensures:
 * 1. No two transactions have the same timestamp (tie-breaking via node_id)
 * 2. Wait-die can correctly order transactions
 * 3. Canonical mode can be maintained based on timestamp ordering
 */

#include "../__dep__.h"
#include <atomic>
#include <cstdint>

namespace janus {

/**
 * LogicalTimestamp represents a globally unique, totally ordered timestamp
 * 
 * Format: (clock, node_id) where:
 * - clock: Lamport logical clock value
 * - node_id: Unique identifier for the coordinator/shard (tie-breaker)
 * 
 * Comparison uses clock first, then node_id for ties.
 * This guarantees total ordering across all nodes in the distributed system.
 */
struct LogicalTimestamp {
  uint64_t clock;      // Lamport clock value
  uint32_t node_id;    // Coordinator/shard ID for tie-breaking
  
  LogicalTimestamp() : clock(0), node_id(0) {}
  LogicalTimestamp(uint64_t c, uint32_t n) : clock(c), node_id(n) {}
  
  /**
   * Total ordering: compare clock first, then node_id
   */
  bool operator<(const LogicalTimestamp& other) const {
    if (clock != other.clock) return clock < other.clock;
    return node_id < other.node_id;
  }
  
  bool operator<=(const LogicalTimestamp& other) const {
    return *this < other || *this == other;
  }
  
  bool operator>(const LogicalTimestamp& other) const {
    return other < *this;
  }
  
  bool operator>=(const LogicalTimestamp& other) const {
    return other <= *this;
  }
  
  bool operator==(const LogicalTimestamp& other) const {
    return clock == other.clock && node_id == other.node_id;
  }
  
  bool operator!=(const LogicalTimestamp& other) const {
    return !(*this == other);
  }
  
  /**
   * Check if this timestamp is valid (non-zero)
   */
  bool IsValid() const {
    return clock > 0 || node_id > 0;
  }
  
  /**
   * Convert to a single 64-bit value for compact storage
   * Format: upper 32 bits = clock (truncated), lower 32 bits = node_id
   */
  uint64_t ToCompact() const {
    return (clock << 32) | static_cast<uint64_t>(node_id);
  }
  
  /**
   * Create from compact 64-bit representation
   */
  static LogicalTimestamp FromCompact(uint64_t compact) {
    return LogicalTimestamp(compact >> 32, static_cast<uint32_t>(compact & 0xFFFFFFFF));
  }
  
  /**
   * String representation for debugging
   */
  std::string ToString() const {
    return "(" + std::to_string(clock) + "," + std::to_string(node_id) + ")";
  }
};

/**
 * LogicalClock maintains a Lamport logical clock for a single node
 * 
 * Lamport Clock Rules:
 * 1. Before each local event (new transaction): clock++
 * 2. When sending a message: include current timestamp
 * 3. When receiving a message: clock = max(clock, received_ts) + 1
 * 
 * Thread-safe via atomic operations.
 */
class LogicalClock {
public:
  /**
   * Construct a logical clock for a specific node
   * @param node_id Unique identifier for this coordinator/shard
   */
  explicit LogicalClock(uint32_t node_id) : node_id_(node_id), clock_(0) {}
  
  LogicalClock() : node_id_(0), clock_(0) {}
  
  /**
   * Set the node ID (if not set in constructor)
   */
  void SetNodeId(uint32_t node_id) {
    node_id_ = node_id;
  }
  
  /**
   * Get the node ID
   */
  uint32_t GetNodeId() const {
    return node_id_;
  }
  
  /**
   * Get a new timestamp for a new transaction/event
   * Atomically increments the clock and returns the new timestamp
   * 
   * @return LogicalTimestamp with incremented clock and this node's ID
   */
  LogicalTimestamp GetTimestamp() {
    uint64_t new_clock = clock_.fetch_add(1, std::memory_order_relaxed) + 1;
    return LogicalTimestamp(new_clock, node_id_);
  }
  
  /**
   * Peek at current clock value without incrementing
   * Useful for debugging or monitoring
   */
  LogicalTimestamp PeekTimestamp() const {
    return LogicalTimestamp(clock_.load(std::memory_order_relaxed), node_id_);
  }
  
  /**
   * Update clock when receiving a timestamp from another node
   * Implements: clock = max(clock, received_ts.clock) + 1
   * 
   * This maintains Lamport clock causality:
   * If event A happens-before event B, then timestamp(A) < timestamp(B)
   * 
   * @param received_ts Timestamp received from another node
   */
  void Update(const LogicalTimestamp& received_ts) {
    uint64_t current = clock_.load(std::memory_order_relaxed);
    uint64_t new_clock = std::max(current, received_ts.clock) + 1;
    
    // CAS loop to ensure atomicity
    while (!clock_.compare_exchange_weak(current, new_clock,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
      new_clock = std::max(current, received_ts.clock) + 1;
    }
  }
  
  /**
   * Update clock with just a clock value (for compact timestamp handling)
   */
  void Update(uint64_t received_clock) {
    uint64_t current = clock_.load(std::memory_order_relaxed);
    uint64_t new_clock = std::max(current, received_clock) + 1;
    
    while (!clock_.compare_exchange_weak(current, new_clock,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
      new_clock = std::max(current, received_clock) + 1;
    }
  }
  
  /**
   * Get current raw clock value
   */
  uint64_t GetCurrentClock() const {
    return clock_.load(std::memory_order_relaxed);
  }
  
  /**
   * Reset the clock (for testing)
   */
  void Reset() {
    clock_.store(0, std::memory_order_relaxed);
  }

private:
  uint32_t node_id_;
  std::atomic<uint64_t> clock_;
};

/**
 * Global logical clock manager (singleton)
 * 
 * Provides access to the logical clock for any component.
 * The node ID is typically set during initialization based on
 * the coordinator/shard ID.
 */
class LogicalClockManager {
public:
  static LogicalClockManager& Instance() {
    static LogicalClockManager instance;
    return instance;
  }
  
  /**
   * Initialize with a node ID
   */
  void Initialize(uint32_t node_id) {
    clock_.SetNodeId(node_id);
  }
  
  /**
   * Get a new timestamp
   */
  LogicalTimestamp GetTimestamp() {
    return clock_.GetTimestamp();
  }
  
  /**
   * Update clock from received timestamp
   */
  void Update(const LogicalTimestamp& received_ts) {
    clock_.Update(received_ts);
  }
  
  /**
   * Update clock from received clock value
   */
  void Update(uint64_t received_clock) {
    clock_.Update(received_clock);
  }
  
  /**
   * Peek current timestamp
   */
  LogicalTimestamp PeekTimestamp() const {
    return clock_.PeekTimestamp();
  }
  
  /**
   * Get node ID
   */
  uint32_t GetNodeId() const {
    return clock_.GetNodeId();
  }
  
  /**
   * Reset (for testing)
   */
  void Reset() {
    clock_.Reset();
  }

private:
  LogicalClockManager() = default;
  ~LogicalClockManager() = default;
  
  LogicalClockManager(const LogicalClockManager&) = delete;
  LogicalClockManager& operator=(const LogicalClockManager&) = delete;
  
  LogicalClock clock_;
};

} // namespace janus


