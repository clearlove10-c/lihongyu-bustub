//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  auto table_write_set = txn->GetWriteSet();
  for (const auto &table_write_record : *table_write_set) {
    auto rid = table_write_record.rid_;
    auto table_heap = table_write_record.table_heap_;
    auto tuple_meta = table_heap->GetTupleMeta(rid);
    tuple_meta.is_deleted_ = !tuple_meta.is_deleted_;
    table_heap->UpdateTupleMeta(tuple_meta, rid);
  }
  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
