#ifndef _MAKO_LAMPORT_CLOCK_H_
#define _MAKO_LAMPORT_CLOCK_H_

#include <atomic>
#include <cstdint>

/**
 * Lamport Clock for MOCC Wait-Die ordering.
 * 
 * Timestamp structure:
 * - High 48 bits: Monotonically increasing counter
 * - Low 16 bits: Node ID (unique per shard)
 * 
 * This ensures globally unique, totally ordered timestamps across all nodes.
 */
class LamportClock {
public:
  static constexpr uint64_t NODE_ID_BITS = 16;
  static constexpr uint64_t NODE_ID_MASK = (1ULL << NODE_ID_BITS) - 1;
  static constexpr uint64_t COUNTER_SHIFT = NODE_ID_BITS;

  /**
   * Initialize the Lamport clock with a node ID.
   * Must be called once per process before any timestamp operations.
   */
  static void init(uint16_t node_id) {
    node_id_ = node_id;
    counter_.store(0, std::memory_order_relaxed);
  }

  /**
   * Get a new unique timestamp.
   * Thread-safe: uses atomic increment.
   */
  static uint64_t get_timestamp() {
    uint64_t count = counter_.fetch_add(1, std::memory_order_relaxed);
    return make_timestamp(count, node_id_);
  }

  /**
   * Update local clock based on received timestamp (Lamport clock rule).
   * Sets local counter to max(local, received) + 1.
   */
  static void update(uint64_t received_timestamp) {
    uint64_t received_counter = extract_counter(received_timestamp);
    uint64_t current;
    uint64_t desired;
    do {
      current = counter_.load(std::memory_order_relaxed);
      if (current > received_counter) {
        return; // Local is already ahead
      }
      desired = received_counter + 1;
    } while (!counter_.compare_exchange_weak(current, desired,
                                              std::memory_order_relaxed,
                                              std::memory_order_relaxed));
  }

  /**
   * Extract the counter portion from a timestamp.
   */
  static uint64_t extract_counter(uint64_t timestamp) {
    return timestamp >> COUNTER_SHIFT;
  }

  /**
   * Extract the node ID portion from a timestamp.
   */
  static uint16_t extract_node_id(uint64_t timestamp) {
    return static_cast<uint16_t>(timestamp & NODE_ID_MASK);
  }

  /**
   * Compare two timestamps for ordering.
   * Returns true if ts1 < ts2 (ts1 is "older").
   */
  static bool is_older(uint64_t ts1, uint64_t ts2) {
    return ts1 < ts2;
  }

private:
  static uint64_t make_timestamp(uint64_t counter, uint16_t node_id) {
    return (counter << COUNTER_SHIFT) | (static_cast<uint64_t>(node_id) & NODE_ID_MASK);
  }

  static std::atomic<uint64_t> counter_;
  static uint16_t node_id_;
};

// Static member definitions (must be in a .cc file or inline in C++17)
// For header-only, we use inline variables (C++17)
inline std::atomic<uint64_t> LamportClock::counter_{0};
inline uint16_t LamportClock::node_id_{0};

#endif /* _MAKO_LAMPORT_CLOCK_H_ */
