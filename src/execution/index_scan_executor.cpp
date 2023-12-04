//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto catalog = this->GetExecutorContext()->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "error in IndexScanExecutor::Init-1");
  index_oid_t index_oid = plan_->index_oid_;
  index_info_ = catalog->GetIndex(index_oid);
  ts_ = this->GetExecutorContext()->GetTransaction();
  auto tree_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  table_info_ = catalog->GetTable(index_info_->table_name_);
  iter_.emplace(tree_index->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!iter_.has_value() || iter_->IsEnd()) {
    return false;
  }
  *rid = (**iter_).second;
  auto record = table_info_->table_->GetTuple(*rid);
  *tuple = record.second;
  ++(*iter_);

  return true;
}

}  // namespace bustub
