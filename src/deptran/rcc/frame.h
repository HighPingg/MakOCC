#pragma once

#include "../frame.h"
#include "../constants.h"
#include "commo.h"

namespace janus {

class FrameRococo : public Frame {
 public:
  FrameRococo(int m=MODE_RCC) : Frame(MODE_RCC) {}
  Executor *CreateExecutor(cmdid_t, TxLogServer *sched) override;
  Coordinator *CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 shared_ptr<TxnRegistry>) override;
  TxLogServer *CreateScheduler() override;
  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                           TxLogServer *dtxn_sched,
                                           rrr::PollThread *poll_mgr,
                                           ServerControlServiceImpl *scsi)
  override;
  mdb::Row *CreateRow(const mdb::Schema *schema,
                      vector<Value> &row_data) override;

  shared_ptr<Tx> CreateTx(epoch_t epoch, txnid_t tid,
                          bool ro, TxLogServer *mgr) override;

  Communicator *CreateCommo(PollThread *poll = nullptr) override;

};
} // namespace janus
