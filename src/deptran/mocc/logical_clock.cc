/**
 * MOCC Logical Clock Implementation
 * 
 * Most of the implementation is in the header file (inline for performance).
 * This file exists for potential future extensions and to ensure the
 * translation unit is compiled.
 */

#include "logical_clock.h"

namespace janus {

// The LogicalClockManager singleton is instantiated in the header.
// This file ensures the translation unit is linked.

// Future extensions could include:
// - Persistent clock state for crash recovery
// - Clock synchronization protocols
// - Clock drift detection and correction

} // namespace janus


