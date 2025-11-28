#pragma once

/**
 * MOCC Frame
 * 
 * Frame class for registering MOCC protocol with the system.
 */

#include "../frame.h"

namespace janus {

class FrameMocc : public Frame {
public:
  FrameMocc() : Frame(MODE_MOCC) {}
  
  /**
   * Create MOCC coordinator
   */
  Coordinator* CreateCoordinator(cooid_t coo_id,
                                 Config* config,
                                 int benchmark,
                                 ClientControlServiceImpl* ccsi,
                                 uint32_t id,
                                 shared_ptr<TxnRegistry> txn_reg) override;
  
  /**
   * Create MOCC scheduler
   */
  TxLogServer* CreateScheduler() override;
  
  /**
   * Create MOCC transaction
   */
  shared_ptr<Tx> CreateTx(epoch_t epoch, txnid_t tid,
                          bool ro, TxLogServer* mgr) override;
  
  /**
   * Create versioned rows (same as OCC)
   */
  mdb::Row* CreateRow(const mdb::Schema* schema,
                      vector<Value>& row_data) override;
};

} // namespace janus




