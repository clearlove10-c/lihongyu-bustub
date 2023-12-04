//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <optional>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  auto catalog = this->GetExecutorContext()->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "error in UpdateExecutor::Init-1");
  table_oid_t table_oid = plan_->table_oid_;
  table_info_ = catalog->GetTable(table_oid);
  indexes_info_ = catalog->GetTableIndexes(table_info_->name_);
  ts_ = this->GetExecutorContext()->GetTransaction();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (updated_) {
    return false;
  }
  Tuple old_tuple;
  RID old_rid;
  Tuple new_tuple;
  std::optional<RID> new_rid;
  int32_t affected_count = 0;
  TupleMeta insert_meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
  TupleMeta delete_meta{INVALID_TXN_ID, INVALID_TXN_ID, true};
  // Get the old tuple
  bool status = child_executor_->Next(&old_tuple, &old_rid);
  while (status) {
    // delete old
    table_info_->table_->UpdateTupleMeta(delete_meta, old_rid);
    // generate new tuple
    std::vector<Value> values{};
    // TODO(SMLZ) check here
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      // 包含了column_value_expr（保留原值）和const_expr（更新值）
      values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }
    // TODO(smlz) check here
    new_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
    // insert_new
    new_rid = table_info_->table_->InsertTuple(insert_meta, new_tuple);
    if (new_rid == std::nullopt) {
      return false;
    }
    // update index
    for (auto index : indexes_info_) {
      Tuple index_tuple;
      index_tuple =
          old_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(index_tuple, old_rid, ts_);
      index_tuple =
          new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(index_tuple, new_rid.value(), ts_);
    }
    affected_count++;
    status = child_executor_->Next(&old_tuple, &old_rid);
  }

  *tuple = {{Value(INTEGER, affected_count)}, &GetOutputSchema()};
  updated_ = true;
  return true;
}
}  // namespace bustub
