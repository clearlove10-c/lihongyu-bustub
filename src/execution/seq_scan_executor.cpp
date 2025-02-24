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
  auto executor_context = this->GetExecutorContext();
  auto lock_manager = executor_context->GetLockManager();
  auto txn = this->GetExecutorContext()->GetTransaction();
  BUSTUB_ASSERT(lock_manager != nullptr, "lock manager is null");
  if (executor_context->IsDelete()) {
    // 删除操作需要为表添加IX锁
    try {
      lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
    } catch (TransactionAbortException &) {
      throw ExecutionException("fail to lock table");
    }
  } else {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
        break;
      case IsolationLevel::REPEATABLE_READ:
      case IsolationLevel::READ_COMMITTED:
        if (exec_ctx_->GetTransaction()->GetIntentionExclusiveTableLockSet()->count(plan_->table_oid_) == 0 &&
            exec_ctx_->GetTransaction()->GetExclusiveTableLockSet()->count(plan_->table_oid_) == 0) {
          // 需要为表添加IS锁
          try {
            lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, plan_->table_oid_);
          } catch (TransactionAbortException &) {
            throw ExecutionException("fail to lock table");
          }
        }
        break;
    }
  }
  // initialize tabel_info_ and iter_
  auto catalog = this->GetExecutorContext()->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "error in SeqScanExecutor::Init-1");
  tabel_info_ = catalog->GetTable(plan_->table_oid_);
  iter_.emplace(tabel_info_->table_->MakeEagerIterator());
}

// auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   auto executor_context = this->GetExecutorContext();
//   auto lock_manager = executor_context->GetLockManager();
//   auto txn = this->GetExecutorContext()->GetTransaction();
//   BUSTUB_ASSERT(lock_manager != nullptr, "lock manager is null");

//   auto unlock_table = [&txn, &lock_manager]() {
//     // 提前释放表锁
//     auto table_s_lock_set = txn->GetSharedTableLockSet();
//     auto table_x_lock_set = txn->GetExclusiveTableLockSet();
//     auto table_is_lock_set = txn->GetIntentionSharedTableLockSet();
//     auto table_ix_lock_set = txn->GetIntentionExclusiveTableLockSet();
//     auto table_six_lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
//     std::unordered_set<table_oid_t> table_lock_set;
//     table_lock_set.insert(table_s_lock_set->begin(), table_s_lock_set->end());
//     table_lock_set.insert(table_x_lock_set->begin(), table_x_lock_set->end());
//     table_lock_set.insert(table_is_lock_set->begin(), table_is_lock_set->end());
//     table_lock_set.insert(table_ix_lock_set->begin(), table_ix_lock_set->end());
//     table_lock_set.insert(table_six_lock_set->begin(), table_six_lock_set->end());
//     for (const auto &table_oid : table_lock_set) {
//       try {
//         lock_manager->UnlockTable(txn, table_oid);
//       } catch (TransactionAbortException &) {
//         throw ExecutionException("fail to unlock table");
//       }
//     }
//   };

//   if (!iter_.has_value() || iter_->IsEnd()) {
//     if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
//       // 提前释放锁
//       unlock_table();
//     }
//     return false;
//   }

//   auto record = iter_->GetTuple();
//   // if (!record.first.is_deleted_) {
//   //   // add lock
//   //   if (executor_context->IsDelete()) {
//   //     // 删除操作需要为行添加X锁
//   //     try {
//   //       lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, iter_->GetRID());
//   //     } catch (TransactionAbortException &) {
//   //       throw ExecutionException("fail to lock row");
//   //     }
//   //   } else {
//   //     switch (txn->GetIsolationLevel()) {
//   //       case IsolationLevel::READ_UNCOMMITTED:
//   //         break;
//   //       case IsolationLevel::REPEATABLE_READ:
//   //       case IsolationLevel::READ_COMMITTED:
//   //         // 需要为行添加S锁
//   //         try {
//   //           lock_manager->LockRow(txn, LockManager::LockMode::SHARED, plan_->table_oid_, iter_->GetRID());
//   //           std::cout << "RID: " << iter_->GetRID() << " take s lock" << std::endl;
//   //         } catch (TransactionAbortException &) {
//   //           throw ExecutionException("fail to lock row");
//   //         }
//   //         break;
//   //     }
//   //   }
//   //   // get data
//   //   *tuple = record.second;
//   //   *rid = iter_->GetRID();
//   //   // unlock row
//   //   if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
//   //     try {
//   //       lock_manager->UnlockRow(txn, plan_->table_oid_, iter_->GetRID(), true);
//   //     } catch (TransactionAbortException &) {
//   //       throw ExecutionException("fail to lock row");
//   //     }
//   //   }
//   //   // add iter
//   //   ++(*iter_);
//   //   return true;
//   // }

//   while (record.first.is_deleted_) {
//     ++(*iter_);
//     if (iter_->IsEnd()) {
//       if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
//         // 提前释放锁
//         unlock_table();
//       }
//       return false;
//     }
//     // add lock
//     if (executor_context->IsDelete()) {
//       // 删除操作需要为行添加X锁
//       try {
//         lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, iter_->GetRID());
//       } catch (TransactionAbortException &) {
//         throw ExecutionException("fail to lock row");
//       }
//     } else {
//       switch (txn->GetIsolationLevel()) {
//         case IsolationLevel::READ_UNCOMMITTED:
//           break;
//         case IsolationLevel::REPEATABLE_READ:
//         case IsolationLevel::READ_COMMITTED:
//           // 需要为行添加S锁
//           try {
//             lock_manager->LockRow(txn, LockManager::LockMode::SHARED, plan_->table_oid_, iter_->GetRID());
//             std::cout << "RID: " << iter_->GetRID() << " take s lock" << std::endl;
//           } catch (TransactionAbortException &) {
//             throw ExecutionException("fail to lock row");
//           }
//           break;
//       }
//     }
//     record = iter_->GetTuple();
//     // skip deleted tuple
//     if (record.first.is_deleted_) {
//       try {
//         lock_manager->UnlockRow(txn, plan_->table_oid_, iter_->GetRID(), true);
//       } catch (TransactionAbortException &) {
//         throw ExecutionException("fail to lock row");
//       }
//       continue;
//     }
//     // unlock row
//     if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
//       try {
//         lock_manager->UnlockRow(txn, plan_->table_oid_, iter_->GetRID(), true);
//       } catch (TransactionAbortException &) {
//         throw ExecutionException("fail to lock row");
//       }
//     }
//   }

//   *tuple = record.second;
//   *rid = iter_->GetRID();
//   ++(*iter_);

//   return true;
// }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!iter_.has_value()) {
    return false;
  }
  while (true) {
    if (iter_->IsEnd()) {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      // 在 READ_COMMITTED 下，在 Next() 函数中，若表中已经没有数据，则提前释放之前持有的锁
      if (iso_level == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
        // 提前释放表锁
        auto table_s_lock_set = exec_ctx_->GetTransaction()->GetSharedTableLockSet();
        auto table_x_lock_set = exec_ctx_->GetTransaction()->GetExclusiveTableLockSet();
        auto table_is_lock_set = exec_ctx_->GetTransaction()->GetIntentionSharedTableLockSet();
        auto table_ix_lock_set = exec_ctx_->GetTransaction()->GetIntentionExclusiveTableLockSet();
        auto table_six_lock_set = exec_ctx_->GetTransaction()->GetSharedIntentionExclusiveTableLockSet();
        std::unordered_set<table_oid_t> table_lock_set;
        table_lock_set.insert(table_s_lock_set->begin(), table_s_lock_set->end());
        table_lock_set.insert(table_x_lock_set->begin(), table_x_lock_set->end());
        table_lock_set.insert(table_is_lock_set->begin(), table_is_lock_set->end());
        table_lock_set.insert(table_ix_lock_set->begin(), table_ix_lock_set->end());
        table_lock_set.insert(table_six_lock_set->begin(), table_six_lock_set->end());
        for (const auto &table_oid : table_lock_set) {
          try {
            exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_oid);
          } catch (TransactionAbortException &) {
            throw ExecutionException("fail to unlock table");
          }
        }
      }
      return false;
    }

    *tuple = iter_->GetTuple().second;
    // 如果 exec_ctx_->IsDelete() 为 true，对于 tuple 就需要获取 EXCLUSIVE 锁
    if (exec_ctx_->IsDelete()) {
      try {
        exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                             plan_->table_oid_, tuple->GetRid());
      } catch (TransactionAbortException &) {
        throw ExecutionException("fail to lock row");
      }
    } else {
      // 如果 IsDelete() 为 false 并且 当前行未被 X lock 锁住 并且隔离级别不为 READ_UNCOMMITTED，则对行上 S lock
      auto row_set = (*exec_ctx_->GetTransaction()->GetExclusiveRowLockSet())[plan_->table_oid_];
      if (row_set.count(tuple->GetRid()) == 0) {
        auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
        // 对于读如果不是READ_UNCOMMITTED就需要获取 SHARED锁
        if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
          try {
            exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                 plan_->table_oid_, tuple->GetRid());
          } catch (TransactionAbortException &) {
            throw ExecutionException("fail to lock row");
          }
        }
      }
    }

    if (iter_->GetTuple().first.is_deleted_) {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level != IsolationLevel::READ_UNCOMMITTED) {
        try {
          exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->table_oid_, tuple->GetRid(), true);
        } catch (TransactionAbortException &) {
          throw ExecutionException("fail to lock row");
        }
      }
      ++(*iter_);
      continue;
    }
    auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
    // 如果 IsDelete() 为 false 并且 隔离级别为 READ_COMMITTED ，还可以释放所有的 S 锁
    if (iso_level == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
      try {
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->table_oid_, tuple->GetRid(), false);
      } catch (TransactionAbortException &) {
        throw ExecutionException("fail to lock row");
      }
    }

    *tuple = iter_->GetTuple().second;
    *rid = tuple->GetRid();
    break;
  }

  ++(*iter_);
  return true;
}

}  // namespace bustub
