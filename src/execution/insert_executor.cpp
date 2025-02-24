//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  auto catalog = this->GetExecutorContext()->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "error in InsertExecutor::Init-1");
  table_oid_t table_oid = plan_->table_oid_;
  table_info_ = catalog->GetTable(table_oid);
  indexes_info_ = catalog->GetTableIndexes(table_info_->name_);
  ts_ = this->GetExecutorContext()->GetTransaction();

  auto lock_manager = this->GetExecutorContext()->GetLockManager();
  BUSTUB_ASSERT(lock_manager != nullptr, "lock manager is null");
  try {
    lock_manager->LockTable(ts_, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid);
  } catch (TransactionAbortException &) {
    throw ExecutionException("fail to lock table");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_) {
    return false;
  }
  Tuple child_tuple{};
  int32_t affected_count = 0;
  TupleMeta meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
  // Get the next tuple
  bool status = child_executor_->Next(&child_tuple, rid);
  while (status) {
    auto lock_manager = this->GetExecutorContext()->GetLockManager();
    auto insert_rid = table_info_->table_->InsertTuple(meta, child_tuple, lock_manager, ts_, plan_->table_oid_);
    if (insert_rid == std::nullopt) {
      return false;
    }
    ts_->AppendTableWriteRecord(TableWriteRecord(ts_->GetTransactionId(), *insert_rid, table_info_->table_.get()));
    // 每次插入都要更新index （之前没有导致只能查到最后一次插入）
    // TODO(smlz) update index (check here)
    // 插入索引的键（tuple）需要用KeyFromTuple来获取
    for (auto index : indexes_info_) {
      Tuple index_tuple = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                                   index->index_->GetKeyAttrs());
      index->index_->InsertEntry(index_tuple, *insert_rid, ts_);
      // DEBUG_INTO_FUNCTION();
      // std::cout << "index name: " << index->name_ << std::endl;
      // std::cout << "key: " << index_tuple.ToString(&index->key_schema_) << std::endl;
    }
    affected_count++;
    status = child_executor_->Next(&child_tuple, rid);
  }

  *tuple = {{Value(INTEGER, affected_count)}, &GetOutputSchema()};
  inserted_ = true;
  return true;
}

}  // namespace bustub
