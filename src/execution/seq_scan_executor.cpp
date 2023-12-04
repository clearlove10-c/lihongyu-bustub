//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // initialize tabel_info_ and iter_
  auto catalog = this->GetExecutorContext()->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "error in SeqScanExecutor::Init-1");
  tabel_info_ = catalog->GetTable(plan_->table_oid_);
  iter_.emplace(tabel_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!iter_.has_value() || iter_->IsEnd()) {
    return false;
  }

  auto record = iter_->GetTuple();
  while (record.first.is_deleted_) {
    ++(*iter_);
    if (iter_->IsEnd()) {
      return false;
    }
    record = iter_->GetTuple();
  }
  *tuple = record.second;
  *rid = iter_->GetRID();
  ++(*iter_);

  return true;
}

}  // namespace bustub
