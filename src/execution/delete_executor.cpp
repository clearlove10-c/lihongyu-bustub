//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  auto catalog = this->GetExecutorContext()->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "error in DeleteExecutor::Init-1");
  table_oid_t table_oid = plan_->table_oid_;
  table_info_ = catalog->GetTable(table_oid);
  indexes_info_ = catalog->GetTableIndexes(table_info_->name_);
  ts_ = this->GetExecutorContext()->GetTransaction();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (deleted_) {
    return false;
  }
  Tuple old_tuple{};
  RID old_rid;
  int32_t affected_count = 0;
  TupleMeta delete_meta{INVALID_TXN_ID, INVALID_TXN_ID, true};
  // Get the old tuple
  bool status = child_executor_->Next(&old_tuple, &old_rid);
  while (status) {
    // delete old
    table_info_->table_->UpdateTupleMeta(delete_meta, old_rid);
    // update write set
    ts_->AppendTableWriteRecord(TableWriteRecord(ts_->GetTransactionId(), old_rid, table_info_->table_.get()));
    // update index
    for (auto index : indexes_info_) {
      Tuple index_tuple =
          old_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(index_tuple, old_rid, ts_);
    }
    affected_count++;
    status = child_executor_->Next(&old_tuple, &old_rid);
  }

  *tuple = {{Value(INTEGER, affected_count)}, &GetOutputSchema()};
  deleted_ = true;
  return true;
}

}  // namespace bustub
