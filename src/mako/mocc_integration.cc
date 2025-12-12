/**
 * MOCC Integration Implementation
 */

#include "mocc_integration.h"

namespace mocc_integration {

// Thread-local storage for per-transaction MOCC state
thread_local MoccTxnState g_mocc_txn_state;

// Global flag to enable/disable MOCC integration
std::atomic<bool> g_mocc_enabled{false};

} // namespace mocc_integration
