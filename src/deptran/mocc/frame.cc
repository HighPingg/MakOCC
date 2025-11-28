/**
 * MOCC Frame Implementation
 */

#include "frame.h"
#include "coordinator.h"
#include "scheduler.h"
#include "tx.h"
#include "../config.h"

namespace janus {

Coordinator* FrameMocc::CreateCoordinator(cooid_t coo_id,
                                          Config* config,
                                          int benchmark,
                                          ClientControlServiceImpl* ccsi,
                                          uint32_t id,
                                          shared_ptr<TxnRegistry> txn_reg) {
  auto* coo = new CoordinatorMocc(coo_id, benchmark, ccsi, id);
  coo->txn_reg_ = txn_reg;
  coo->frame_ = this;
  return coo;
}

TxLogServer* FrameMocc::CreateScheduler() {
  auto* sch = new SchedulerMocc();
  sch->frame_ = this;
  return sch;
}

shared_ptr<Tx> FrameMocc::CreateTx(epoch_t epoch, txnid_t tid,
                                   bool ro, TxLogServer* mgr) {
  return make_shared<TxMocc>(epoch, tid, mgr);
}

mdb::Row* FrameMocc::CreateRow(const mdb::Schema* schema,
                               vector<Value>& row_data) {
  // MOCC uses versioned rows (same as OCC)
  return mdb::VersionedRow::create(schema, row_data);
}

// Register MOCC frame with the system
static Frame* __mocc_frame = Frame::RegFrame(
    MODE_MOCC,
    {"mocc", "MOCC"},
    []() -> Frame* { return new FrameMocc(); }
);

} // namespace janus




