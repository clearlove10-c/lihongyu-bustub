//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <shared_mutex>
#include <stack>
#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // std::cout << "txn: " << txn->GetTransactionId() << " lock table: " << oid
  //           << " in lock mode: " << static_cast<int>(lock_mode) << std::endl;
  // 1. check if transaction can qauire for this lock
  if (!CanTxnTakeTableLock(txn, lock_mode)) {
    return false;
  }
  // 2. get table-lock held by current transaction
  auto current_lock_mode = GetCurrentTableLockMode(txn, oid);
  if (current_lock_mode != std::nullopt) {
    // 3. current transaction already holds a lock, upgrade needed
    return UpgradeLockTable(txn, current_lock_mode.value(), lock_mode, oid);
  }
  // 3. simply aquire a lock for current transaction
  return AquireLockTable(txn, lock_mode, oid);
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::cout << "txn: " << txn->GetTransactionId() << " unlock table: " << oid << std::endl;
  // 1. get block-queue for current resource(table)
  table_lock_map_latch_.lock();
  auto lock_request_queue_ptr = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  if (lock_request_queue_ptr == nullptr) {
    TransactionLatchGuard txn_lock(txn);
    txn->SetState(TransactionState::ABORTED);
    std::cout << "abort 1" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  // 2. get request_queue from LockRequestQueue
  std::unique_lock<std::mutex> block_queue_lock(lock_request_queue_ptr->latch_);
  auto &request_queue = lock_request_queue_ptr->request_queue_;
  // 3. find current_lock_request in block-queue
  txn_id_t current_txn_id = txn->GetTransactionId();
  auto current_request_iter = std::find_if(
      request_queue.begin(), request_queue.end(),
      [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
  if (current_request_iter == request_queue.end() || !(*current_request_iter)->granted_) {
    // 4. current transaction didn't hold a lock
    TransactionLatchGuard txn_lock(txn);
    txn->SetState(TransactionState::ABORTED);
    std::cout << "abort 2" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  // 5. check if transaction holds a lock on row
  TransactionLatchGuard txn_lock(txn);
  if (auto txn_row_lock_set = txn->GetSharedRowLockSet();
      txn_row_lock_set != nullptr && (!(*txn_row_lock_set)[oid].empty())) {
    // 6. transaction holds a shared lock on one row of current table
    txn->SetState(TransactionState::ABORTED);
    std::cout << "abort 3" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }
  if (auto txn_row_lock_set = txn->GetExclusiveRowLockSet();
      txn_row_lock_set != nullptr && (!(*txn_row_lock_set)[oid].empty())) {
    // 6. transaction holds a shared lock on one row of current table
    txn->SetState(TransactionState::ABORTED);
    std::cout << "abort 4" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }
  BUSTUB_ASSERT((*current_request_iter) != nullptr, "nullptr error");
  LockMode current_lock_mode = (*current_request_iter)->lock_mode_;
  // 6. get table-lock-set
  std::shared_ptr<std::unordered_set<table_oid_t>> txn_table_lock_set_ptr;
  switch (current_lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn_table_lock_set_ptr = txn->GetIntentionSharedTableLockSet();
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn_table_lock_set_ptr = txn->GetSharedIntentionExclusiveTableLockSet();
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn_table_lock_set_ptr = txn->GetIntentionExclusiveTableLockSet();
      break;
    case LockMode::EXCLUSIVE:
      txn_table_lock_set_ptr = txn->GetExclusiveTableLockSet();
      break;
    case LockMode::SHARED:
      txn_table_lock_set_ptr = txn->GetSharedTableLockSet();
      break;
  }
  BUSTUB_ASSERT(txn_table_lock_set_ptr != nullptr, "error here");
  // 7. update(delete) lock-set
  txn_table_lock_set_ptr->erase(oid);
  // 8. update block-queue(in other word: drop previous held lock request)
  request_queue.erase(current_request_iter);
  // 9 notify cv
  lock_request_queue_ptr->cv_.notify_all();
  // 8. update transaction state
  if (current_lock_mode == LockMode::SHARED) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_COMMITTED:
        break;
      case IsolationLevel::READ_UNCOMMITTED:
        throw bustub::Exception("s lock not not permitted under READ_UNCOMMITTED");
        break;
      case IsolationLevel::REPEATABLE_READ:
        txn->SetState(TransactionState::SHRINKING);
    }
  } else if (current_lock_mode == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
  }
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // std::cout << "txn: " << txn->GetTransactionId() << " lock row: " << rid << " in table: " << oid
  //           << " in lock mode: " << static_cast<int>(lock_mode) << std::endl;
  // 1. check if transaction can qauire for this lock
  if (!CanTxnTakeRowLock(txn, lock_mode)) {
    return false;
  }
  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    return false;
  }
  // 2. get row-lock held by current transaction
  auto current_lock_mode = GetCurrentRowLockMode(txn, oid, rid);
  if (current_lock_mode != std::nullopt) {
    // 3. current transaction already holds a lock, upgrade needed
    return UpgradeLockRow(txn, current_lock_mode.value(), lock_mode, oid, rid);
  }
  // 3. simply aquire a lock for current transaction
  return AquireLockRow(txn, lock_mode, oid, rid);
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  std::cout << "txn: " << txn->GetTransactionId() << " unlock row: " << rid << " in table: " << oid << std::endl;
  // 1. get block-queue for current resource(row)
  row_lock_map_latch_.lock();
  auto lock_request_queue_ptr = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  if (lock_request_queue_ptr == nullptr) {
    TransactionLatchGuard txn_lock(txn);
    txn->SetState(TransactionState::ABORTED);
    std::cout << "abort 5" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  // 2. get request_queue from LockRequestQueue
  std::unique_lock<std::mutex> block_queue_lock(lock_request_queue_ptr->latch_);
  auto &request_queue = lock_request_queue_ptr->request_queue_;
  // 3. find current_lock_request in block-queue
  txn_id_t current_txn_id = txn->GetTransactionId();
  auto current_request_iter = std::find_if(
      request_queue.begin(), request_queue.end(),
      [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
  if (current_request_iter == request_queue.end() || !(*current_request_iter)->granted_) {
    // 4. current transaction didn't hold a lock
    TransactionLatchGuard txn_lock(txn);
    txn->SetState(TransactionState::ABORTED);
    std::cout << "abort 6" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  BUSTUB_ASSERT((*current_request_iter) != nullptr, "nullptr error");
  LockMode current_lock_mode = (*current_request_iter)->lock_mode_;
  TransactionLatchGuard txn_lock(txn);
  // 6. get row-lock-set
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> txn_row_lock_set_ptr;
  switch (current_lock_mode) {
    case LockMode::INTENTION_SHARED:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
    case LockMode::INTENTION_EXCLUSIVE:
      throw bustub::Exception("there should on be intension lock in row level");
      break;
    case LockMode::EXCLUSIVE:
      txn_row_lock_set_ptr = txn->GetExclusiveRowLockSet();
      break;
    case LockMode::SHARED:
      txn_row_lock_set_ptr = txn->GetSharedRowLockSet();
      break;
  }
  BUSTUB_ASSERT(txn_row_lock_set_ptr != nullptr, "error here");
  // 7. update(delete) lock-set
  (*txn_row_lock_set_ptr)[oid].erase(rid);
  // 8. update block-queue(in other word: drop previous held lock request)
  request_queue.erase(current_request_iter);
  // 9 notify cv
  lock_request_queue_ptr->cv_.notify_all();
  // 8. update transaction state
  if (!force) {
    if (current_lock_mode == LockMode::SHARED) {
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::READ_COMMITTED:
          break;
        case IsolationLevel::READ_UNCOMMITTED:
          throw bustub::Exception("s lock not not permitted under READ_UNCOMMITTED");
          break;
        case IsolationLevel::REPEATABLE_READ:
          txn->SetState(TransactionState::SHRINKING);
      }
    } else if (current_lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> waits_for_lock(waits_for_latch_);
  if (std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2) != waits_for_[t1].end()) {
    // std::cout << "add existing edge" << std::endl;
    return;
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> waits_for_lock(waits_for_latch_);
  auto it = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (it == waits_for_[t1].end()) {
    std::cout << " no such edge to delete" << std::endl;
    return;
  }
  waits_for_[t1].erase(it);
}

void LockManager::DFS(txn_id_t current_txn, std::vector<txn_id_t> &cycle_stack,
                      std::vector<std::vector<txn_id_t>> &all_cycles, std::map<txn_id_t, bool> &in_cycle,
                      std::map<txn_id_t, bool> &visited) {
  if (in_cycle[current_txn]) {
    std::vector<txn_id_t> current_cycle;
    // std::cout << "found cycle: ";
    size_t start_index = 0;
    while (cycle_stack[start_index] != current_txn) {
      ++start_index;
    }
    for (; start_index < cycle_stack.size(); ++start_index) {
      current_cycle.push_back(cycle_stack[start_index]);
      // std::cout << cycle_stack[start_index] << " ";
    }
    // std::cout << std::endl;
    all_cycles.push_back(current_cycle);
    return;
  }

  // std::cout << "visit node: " << current_txn << std::endl;
  in_cycle[current_txn] = true;
  cycle_stack.push_back(current_txn);
  visited[current_txn] = true;

  if (waits_for_[current_txn].empty()) {
    // 当前节点无后继，此次未发现圈
    cycle_stack.pop_back();
    in_cycle[current_txn] = false;
    return;
  }

  for (const auto &neighbor : waits_for_[current_txn]) {
    DFS(neighbor, cycle_stack, all_cycles, in_cycle, visited);
  }
  in_cycle[current_txn] = false;
  cycle_stack.pop_back();
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unique_lock<std::mutex> waits_for_lock(waits_for_latch_);
  auto start_txn = std::numeric_limits<txn_id_t>::max();
  for (auto &uvs : waits_for_) {
    if (!uvs.second.empty()) {
      start_txn = std::min(start_txn, uvs.first);
      std::sort(uvs.second.begin(), uvs.second.end());
    }
  }
  std::map<txn_id_t, bool> visited;
  for (const auto &pair : waits_for_) {
    visited[pair.first] = false;
  }
  std::map<txn_id_t, bool> in_cycle;
  std::vector<txn_id_t> cycle_stack;
  std::vector<std::vector<txn_id_t>> all_cycles;

  auto find_next_start = [](const std::map<txn_id_t, bool> &visited, txn_id_t &start_txn) {
    for (auto node : visited) {
      if (!node.second) {
        start_txn = node.first;
        return true;
      }
    }
    return false;
  };

  do {
    DFS(start_txn, cycle_stack, all_cycles, in_cycle, visited);
  } while (all_cycles.empty() && find_next_start(visited, start_txn));

  if (all_cycles.empty()) {
    return false;
  }
  std::sort(all_cycles[0].begin(), all_cycles[0].end(), std::greater<>());
  BUSTUB_ASSERT(!all_cycles[0].empty(), "should have at least two nodes in cycle");
  *txn_id = all_cycles[0][0];
  return true;
}

// 可以是乱序的，测试时会排序
auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  std::unique_lock<std::mutex> waits_for_lock(waits_for_latch_);
  for (const auto &kvs : waits_for_) {
    if (!kvs.second.empty()) {
      for (auto v : kvs.second) {
        edges.emplace_back(kvs.first, v);
      }
    }
  }
  return edges;
}

// 调用它时再开启线程，该函数中不用创建线程
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      // TODO(SMLZ) 讲义上说不需要latch?
      std::unique_lock<std::mutex> table_map_lock(table_lock_map_latch_);
      std::unique_lock<std::mutex> row_map_lock(row_lock_map_latch_);
      // construct table lock
      for (const auto &kv : table_lock_map_) {
        std::set<std::shared_ptr<LockRequest>> granted_requests;
        if (auto request_queue_ptr = kv.second; request_queue_ptr != nullptr) {
          // init granted requests
          for (const auto &request_ptr : request_queue_ptr->request_queue_) {
            if (request_ptr != nullptr && request_ptr->granted_) {
              BUSTUB_ASSERT(txn_manager_->GetTransaction(request_ptr->txn_id_)->GetState() != TransactionState::ABORTED,
                            "granted txn should not be aborted");
              granted_requests.insert(request_ptr);
            }
          }
          // add edges
          for (const auto &request_ptr : request_queue_ptr->request_queue_) {
            if (request_ptr != nullptr && !request_ptr->granted_) {
              for (const auto &granted_request : granted_requests) {
                if (!AreLocksCompatible(request_ptr->lock_mode_, granted_request->lock_mode_)) {
                  if (txn_manager_->GetTransaction(request_ptr->txn_id_)->GetState() == TransactionState::ABORTED) {
                    continue;
                  }
                  // current txn wait for granted txn
                  // // TODO(SMLZ) 一个事务只能同时等待一个资源吧？（并不是）
                  // BUSTUB_ASSERT(txn_to_block_queue[request_ptr->txn_id_] == nullptr, "ont txn wait for one
                  // resource");
                  AddEdge(request_ptr->txn_id_, granted_request->txn_id_);
                }
              }
            }
          }
        }
      }
      // construct row lock
      for (const auto &kv : row_lock_map_) {
        std::set<std::shared_ptr<LockRequest>> granted_requests;
        if (auto request_queue_ptr = kv.second; request_queue_ptr != nullptr) {
          // init granted requests
          for (const auto &request_ptr : request_queue_ptr->request_queue_) {
            if (request_ptr != nullptr && request_ptr->granted_) {
              BUSTUB_ASSERT(txn_manager_->GetTransaction(request_ptr->txn_id_)->GetState() != TransactionState::ABORTED,
                            "granted txn should not be aborted");
              granted_requests.insert(request_ptr);
            }
          }
          // add edges
          for (const auto &request_ptr : request_queue_ptr->request_queue_) {
            if (request_ptr != nullptr && !request_ptr->granted_) {
              for (const auto &granted_request : granted_requests) {
                if (!AreLocksCompatible(request_ptr->lock_mode_, granted_request->lock_mode_)) {
                  // current wait for granted
                  AddEdge(request_ptr->txn_id_, granted_request->txn_id_);
                }
              }
            }
          }
        }
      }
      // 这里要解锁table_map_lock和row_map_lock，因为abort函数会解锁，修改那两个Map
      table_map_lock.unlock();
      row_map_lock.unlock();
      txn_id_t victim_tx_id;
      while (HasCycle(&victim_tx_id)) {
        // abort victim txn
        auto victim_txn = txn_manager_->GetTransaction(victim_tx_id);
        std::cout << "deadlock released! aborted txn id: " << victim_tx_id << std::endl;
        txn_manager_->Abort(victim_txn);
        // notify other txns？？？不用了，txn的abort函数会解锁同时通知其他线程
        waits_for_[victim_tx_id].clear();
      }
    }
  }
}

auto LockManager::AquireLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 1. aquire latch for member: table_lock_map
  table_lock_map_latch_.lock();
  // 2. get block-queue for current resource(table)
  auto lock_request_queue_ptr = table_lock_map_[oid];
  if (lock_request_queue_ptr == nullptr) {
    // 3. block-queue is empty
    // 3.1 init block-queue for current resource
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    // 3.2 refresh block-queue
    lock_request_queue_ptr = table_lock_map_[oid];
  }
  table_lock_map_latch_.unlock();
  BUSTUB_ASSERT(lock_request_queue_ptr != nullptr, "no nullptr allowed here");
  // 3. aquire for lock
  // 3.1. aquire latch for member: lock_request_queue
  std::unique_lock<std::mutex> block_queue_lock(lock_request_queue_ptr->latch_);
  // 3.2. get request_queue from LockRequestQueue
  auto &request_queue = lock_request_queue_ptr->request_queue_;
  // 3.3. generate current request
  auto current_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  // 3.4. add current request to the end of queue
  request_queue.emplace_back(std::move(current_request));

  // judge if can grant lock to current transaction
  const auto can_grant_lock = [&]() -> bool {
    bool is_first_request = false;
    if (lock_request_queue_ptr->upgrading_ != INVALID_TXN_ID) {
      // 1. other transaction is upgrading lock for current resource
      return false;
    }
    for (auto &request_ptr : request_queue) {
      BUSTUB_ASSERT(request_ptr != nullptr, "lock request should not be null");
      if (!request_ptr->granted_) {
        if (is_first_request) {
          continue;
        }
        if (request_ptr->txn_id_ == txn->GetTransactionId()) {
          is_first_request = true;
          continue;
        }
        return false;
      }
      if (!AreLocksCompatible(lock_mode, request_ptr->lock_mode_)) {
        // lock_wanted is imcompatible with this lock
        return false;
      }
    }
    return true;
  };

  auto grant_lock = [&]() -> void {
    // 1. find current_lock_request in block-queue
    BUSTUB_ASSERT(!request_queue.empty(), "request queue shold not be empty");
    txn_id_t current_txn_id = txn->GetTransactionId();
    auto current_request_iter = std::find_if(
        request_queue.begin(), request_queue.end(),
        [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
    BUSTUB_ASSERT(current_request_iter != request_queue.end(), "should have a lock request in block-queue");
    // 2. grant lock to transaction
    // 2.1. update lock-set in transaction
    // 2.1.1 get lock-set
    std::shared_ptr<std::unordered_set<table_oid_t>> txn_table_lock_set_ptr;
    // TransactionLatchGuard txn_lock(txn);
    switch (lock_mode) {
      case LockMode::INTENTION_SHARED:
        txn_table_lock_set_ptr = txn->GetIntentionSharedTableLockSet();
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn_table_lock_set_ptr = txn->GetSharedIntentionExclusiveTableLockSet();
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        txn_table_lock_set_ptr = txn->GetIntentionExclusiveTableLockSet();
        break;
      case LockMode::EXCLUSIVE:
        txn_table_lock_set_ptr = txn->GetExclusiveTableLockSet();
        break;
      case LockMode::SHARED:
        txn_table_lock_set_ptr = txn->GetSharedTableLockSet();
        break;
    }
    BUSTUB_ASSERT(txn_table_lock_set_ptr != nullptr, "error here");
    // 2.1.2 update lock-set
    txn_table_lock_set_ptr->insert(oid);
    // 2.2. update block-queue(in other word: current lock request)
    (*current_request_iter)->granted_ = true;
  };

  if (can_grant_lock()) {
    // 3.5. can grant lock to current transaction
    TransactionLatchGuard txn_lock(txn);
    grant_lock();
    return true;
  }
  while (true) {
    // 3.5. cannot grant lock, wait until notified
    lock_request_queue_ptr->cv_.wait(block_queue_lock);
    // 3.6. notified
    // 惊群效应？
    {
      TransactionLatchGuard txn_lock(txn);
      if (txn->GetState() == TransactionState::ABORTED) {
        // TODO(SMLZ) delete from request-queue?
        txn_id_t current_txn_id = txn->GetTransactionId();
        auto current_request_iter = std::find_if(
            request_queue.begin(), request_queue.end(),
            [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
        request_queue.erase(current_request_iter);
        lock_request_queue_ptr->cv_.notify_all();
        return false;
      }
      if (can_grant_lock()) {
        // 3.7. can grant lock to current transaction
        grant_lock();
        lock_request_queue_ptr->cv_.notify_all();
        return true;
      }
    }
    // 3.7. still cannot grant, keep waiting
  }
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode current_lock_mode, LockMode lock_mode,
                                   const table_oid_t &oid) -> bool {
  std::cout << "upgrade txn: " << txn->GetTransactionId() << " from " << static_cast<int>(current_lock_mode) << " to "
            << static_cast<int>(lock_mode) << std::endl;
  if (!CanLockUpgrade(current_lock_mode, lock_mode)) {
    // 不能在这里允许锁降级（通不过兼容性测试，只能在算子里面进行）
    // if (current_lock_mode == LockMode::INTENTION_EXCLUSIVE || current_lock_mode == LockMode::EXCLUSIVE) {
    //   std::cout << "upgrade from" << static_cast<int>(current_lock_mode) << " to " << static_cast<int>(lock_mode)
    //             << std::endl;
    //   return true;
    // }
    // 1. cannot upgrade lock: INCOMPATIBLE_UPGRADE
    TransactionLatchGuard txn_lock(txn);
    txn->SetState(TransactionState::ABORTED);
    std::cout << "abort 7" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    return false;
  }
  // 1. aquire latch for member: table_lock_map
  table_lock_map_latch_.lock();
  // 2. get block-queue for current resource(table)
  auto lock_request_queue_ptr = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  BUSTUB_ASSERT(lock_request_queue_ptr != nullptr, "error here");
  // 3. aquire latch for member: lock_request_queue
  std::unique_lock<std::mutex> block_queue_lock(lock_request_queue_ptr->latch_);
  if (lock_request_queue_ptr->upgrading_ != INVALID_TXN_ID) {
    // 4. other transaction is upgrading, conflicts!
    TransactionLatchGuard txn_lock(txn);
    txn->SetState(TransactionState::ABORTED);
    std::cout << "abort 8" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  // 4. annonce tha current transaction is upgrading
  lock_request_queue_ptr->upgrading_ = txn->GetTransactionId();
  // 5. get request_queue from LockRequestQueue
  auto &request_queue = lock_request_queue_ptr->request_queue_;
  BUSTUB_ASSERT(!request_queue.empty(), "request queue shold not be empty");
  // 6. find transaction's previous lock-request in block-queue
  txn_id_t current_txn_id = txn->GetTransactionId();
  auto previous_request_iter = std::find_if(
      request_queue.begin(), request_queue.end(),
      [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
  BUSTUB_ASSERT(previous_request_iter != request_queue.end(),
                "there should be a lock-request for current transaction in the queue");
  BUSTUB_ASSERT((*previous_request_iter)->granted_ = true, "transaction should hold a previous lock");
  {
    // 7. drop current lock in transaction
    TransactionLatchGuard txn_lock(txn);
    std::shared_ptr<std::unordered_set<table_oid_t>> txn_table_lock_set_ptr;
    switch (current_lock_mode) {
      case LockMode::INTENTION_SHARED:
        txn_table_lock_set_ptr = txn->GetIntentionSharedTableLockSet();
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn_table_lock_set_ptr = txn->GetSharedIntentionExclusiveTableLockSet();
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        txn_table_lock_set_ptr = txn->GetIntentionExclusiveTableLockSet();
        break;
      case LockMode::EXCLUSIVE:
        txn_table_lock_set_ptr = txn->GetExclusiveTableLockSet();
        break;
      case LockMode::SHARED:
        txn_table_lock_set_ptr = txn->GetSharedTableLockSet();
        break;
    }
    BUSTUB_ASSERT(txn_table_lock_set_ptr != nullptr, "error here");
    // 7.1.2 update lock-set
    txn_table_lock_set_ptr->erase(oid);
    // 7.2. update block-queue(in other word: drop previous held lock request)
    request_queue.erase(previous_request_iter);
    // 7.3 notify cv(useless: because this block-queue is upgrading, no other transaction can get lock)
    lock_request_queue_ptr->cv_.notify_all();
  }
  // 8. aquire for new lock

  // judge if can grant lock to current transaction
  auto can_grant_lock = [&]() -> bool {
    bool can_grant = true;
    BUSTUB_ASSERT(lock_request_queue_ptr->upgrading_ == current_txn_id, "one and only one transaction is upgrading");
    for (auto &request_ptr : request_queue) {
      if (!request_ptr->granted_) {
        continue;
      }
      if (!AreLocksCompatible(lock_mode, request_ptr->lock_mode_)) {
        // lock_wanted is imcompatible with this lock
        can_grant = false;
        break;
      }
    }
    return can_grant;
  };

  auto grant_lock = [&]() -> void {
    // 1. find current_lock_request in block-queue
    // TODO(SMLZ) assume there's only one request in block-queue for one transaction?
    BUSTUB_ASSERT(!request_queue.empty(), "request queue shold not be empty");
    txn_id_t current_txn_id = txn->GetTransactionId();
    auto current_request_iter = std::find_if(
        request_queue.begin(), request_queue.end(),
        [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
    BUSTUB_ASSERT(current_request_iter != request_queue.end(), "should have a lock request in block-queue");
    // 2. grant lock to transaction
    // 2.1. update lock-set in transaction
    // 2.1.1 get lock-set
    std::shared_ptr<std::unordered_set<table_oid_t>> txn_table_lock_set_ptr;
    switch (lock_mode) {
      case LockMode::INTENTION_SHARED:
        txn_table_lock_set_ptr = txn->GetIntentionSharedTableLockSet();
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn_table_lock_set_ptr = txn->GetSharedIntentionExclusiveTableLockSet();
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        txn_table_lock_set_ptr = txn->GetIntentionExclusiveTableLockSet();
        break;
      case LockMode::EXCLUSIVE:
        txn_table_lock_set_ptr = txn->GetExclusiveTableLockSet();
        break;
      case LockMode::SHARED:
        txn_table_lock_set_ptr = txn->GetSharedTableLockSet();
        break;
    }
    BUSTUB_ASSERT(txn_table_lock_set_ptr != nullptr, "error here");
    // 2.1.2 update lock-set
    txn_table_lock_set_ptr->insert(oid);
    // 2.2. update block-queue(in other word: current lock request)
    (*current_request_iter)->granted_ = true;
  };

  // 8.0 generate new lock request
  auto current_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  // 8.0 add current request to the end of queue
  request_queue.emplace_back(std::move(current_request));

  if (can_grant_lock()) {
    // 8.1 can grant lock to current transaction
    TransactionLatchGuard txn_lock(txn);
    grant_lock();
    lock_request_queue_ptr->upgrading_ = INVALID_TXN_ID;
    return true;
  }
  while (true) {
    // 8.2. cannot grant lock, wait until notified
    lock_request_queue_ptr->cv_.wait(block_queue_lock);
    // 8.3 notified
    // 惊群效应？
    {
      TransactionLatchGuard txn_lock(txn);
      if (txn->GetState() == TransactionState::ABORTED) {
        // TODO(SMLZ) delete from request-queue?
        txn_id_t current_txn_id = txn->GetTransactionId();
        auto current_request_iter = std::find_if(
            request_queue.begin(), request_queue.end(),
            [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
        request_queue.erase(current_request_iter);
        lock_request_queue_ptr->upgrading_ = INVALID_TXN_ID;
        lock_request_queue_ptr->cv_.notify_all();
        return false;
      }
      if (can_grant_lock()) {
        // 8.4 can grant lock to current transaction
        grant_lock();
        lock_request_queue_ptr->upgrading_ = INVALID_TXN_ID;
        return true;
      }
    }
    // 8.5 still cannot grant, keep waiting
  }
}

auto LockManager::AquireLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 1. aquire latch for member: row_lock_map
  row_lock_map_latch_.lock();
  // 2. get block-queue for current resource(row)
  auto lock_request_queue_ptr = row_lock_map_[rid];
  if (lock_request_queue_ptr == nullptr) {
    // 3. block-queue is empty
    // 3.1 init block-queue for current resource
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    // 3.2 refersh block-queue
    lock_request_queue_ptr = row_lock_map_[rid];
  }
  row_lock_map_latch_.unlock();
  BUSTUB_ASSERT(lock_request_queue_ptr != nullptr, "no nullptr allowed here");
  // 3. aquire for lock
  // 3.1. aquire latch for member: lock_request_queue
  std::unique_lock<std::mutex> lock(lock_request_queue_ptr->latch_);
  // 3.2. get request_queue from LockRequestQueue
  auto &request_queue = lock_request_queue_ptr->request_queue_;
  // 3.3. generate current request
  auto current_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  // 3.4. add current request to the end of queue
  request_queue.emplace_back(std::move(current_request));

  // judge if can grant lock to current transaction
  auto can_grant_lock = [&]() -> bool {
    bool can_grant = true;
    if (lock_request_queue_ptr->upgrading_ != INVALID_TXN_ID) {
      // 1. other transaction is upgrading lock for current resource
      return false;
    }
    for (auto &request_ptr : request_queue) {
      BUSTUB_ASSERT(request_ptr != nullptr, "error here");
      if (!request_ptr->granted_) {
        continue;
      }
      if (!AreLocksCompatible(lock_mode, request_ptr->lock_mode_)) {
        // lock_wanted is imcompatible with this lock
        can_grant = false;
        break;
      }
    }
    return can_grant;
  };

  auto grant_lock = [&]() -> void {
    // 1. find current_lock_request in block-queue
    BUSTUB_ASSERT(!request_queue.empty(), "request queue shold not be empty");
    txn_id_t current_txn_id = txn->GetTransactionId();
    auto current_request_iter = std::find_if(
        request_queue.begin(), request_queue.end(),
        [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
    BUSTUB_ASSERT(current_request_iter != request_queue.end(), "should have a lock request in block-queue");
    // 2. grant lock to transaction
    // 2.1. update lock-set in transaction
    // 2.1.1 get lock-set
    std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> txn_row_lock_set_ptr;
    // TransactionLatchGuard txn_lock(txn);
    switch (lock_mode) {
      case LockMode::INTENTION_SHARED:
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
      case LockMode::INTENTION_EXCLUSIVE:
        throw bustub::Exception("there should on be intension lock in row level");
        break;
      case LockMode::EXCLUSIVE:
        txn_row_lock_set_ptr = txn->GetExclusiveRowLockSet();
        break;
      case LockMode::SHARED:
        txn_row_lock_set_ptr = txn->GetSharedRowLockSet();
        break;
    }
    BUSTUB_ASSERT(txn_row_lock_set_ptr != nullptr, "error here");
    // 2.1.2 update lock-set
    (*txn_row_lock_set_ptr)[oid].insert(rid);
    // 2.2. update block-queue(in other word: current lock request)
    (*current_request_iter)->granted_ = true;
  };

  if (can_grant_lock()) {
    // 3.5. can grant lock to current transaction
    TransactionLatchGuard txn_lock(txn);
    grant_lock();
    return true;
  }
  while (true) {
    // 3.5. cannot grant lock, wait until notified
    lock_request_queue_ptr->cv_.wait(lock);
    // 3.6. notified
    // 惊群效应？
    {
      TransactionLatchGuard txn_lock(txn);
      if (txn->GetState() == TransactionState::ABORTED) {
        // TODO(SMLZ) delete from request-queue?
        txn_id_t current_txn_id = txn->GetTransactionId();
        auto current_request_iter = std::find_if(
            request_queue.begin(), request_queue.end(),
            [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
        request_queue.erase(current_request_iter);
        return false;
      }
      if (can_grant_lock()) {
        // 3.7. can grant lock to current transaction
        grant_lock();
        return true;
      }
    }
    // 3.7. still cannot grant, keep waiting
  }
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockMode current_lock_mode, LockMode lock_mode,
                                 const table_oid_t &oid, const RID &rid) -> bool {
  if (!CanLockUpgrade(current_lock_mode, lock_mode)) {
    // 1. cannot upgrade lock: INCOMPATIBLE_UPGRADE
    TransactionLatchGuard txn_lock(txn);
    txn->SetState(TransactionState::ABORTED);
    std::cout << "abort 8" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    return false;
  }
  // 1. aquire latch for member: row_lock_map
  row_lock_map_latch_.lock();
  // 2. get block-queue for current resource(row)
  auto lock_request_queue_ptr = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  BUSTUB_ASSERT(lock_request_queue_ptr != nullptr, "error here");
  // 3. aquire latch for member: lock_request_queue
  std::unique_lock<std::mutex> block_queue_lock(lock_request_queue_ptr->latch_);
  if (lock_request_queue_ptr->upgrading_ != INVALID_TXN_ID) {
    // 4. other transaction is upgrading, conflicts!
    TransactionLatchGuard txn_lock(txn);
    txn->SetState(TransactionState::ABORTED);
    std::cout << "abort 9" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  // 4. annonce tha current transaction is upgrading
  lock_request_queue_ptr->upgrading_ = txn->GetTransactionId();
  // 5. get request_queue from LockRequestQueue
  auto &request_queue = lock_request_queue_ptr->request_queue_;
  BUSTUB_ASSERT(!request_queue.empty(), "request queue shold not be empty");
  // 6. find transaction's previous lock-request in block-queue
  txn_id_t current_txn_id = txn->GetTransactionId();
  // TODO(SMLZ) 这里要制定rid和oid吗？一个request_queue应该对应某一行资源，不用再添加了吧
  auto previous_request_iter = std::find_if(
      request_queue.begin(), request_queue.end(),
      [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
  BUSTUB_ASSERT(previous_request_iter != request_queue.end(),
                "there should be a lock-request for current transaction in the queue");
  BUSTUB_ASSERT((*previous_request_iter)->granted_ = true, "transaction should hold a previous lock");
  {
    // 7. drop current lock in transaction
    // 7.1 update lock-set in transaction
    // 7.1.1 get lock-set
    TransactionLatchGuard txn_lock(txn);
    std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> txn_row_lock_set_ptr;
    switch (current_lock_mode) {
      case LockMode::INTENTION_SHARED:
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
      case LockMode::INTENTION_EXCLUSIVE:
        throw bustub::Exception("there should on be intension lock in row level");
        break;
      case LockMode::EXCLUSIVE:
        txn_row_lock_set_ptr = txn->GetExclusiveRowLockSet();
        break;
      case LockMode::SHARED:
        txn_row_lock_set_ptr = txn->GetSharedRowLockSet();
        break;
    }
    BUSTUB_ASSERT(txn_row_lock_set_ptr != nullptr, "error here");
    // 7.1.2 update lock-set
    (*txn_row_lock_set_ptr)[oid].erase(rid);
    if ((*txn_row_lock_set_ptr)[oid].empty()) {
      txn_row_lock_set_ptr->erase(oid);
    }
    // 7.2. update block-queue(in other word: drop previous held lock request)
    request_queue.erase(previous_request_iter);
    // 7.3 notify cv(useless: because this block-queue is upgrading, no other transaction can get lock)
    lock_request_queue_ptr->cv_.notify_all();
  }
  // 8. aquire for new lock

  // judge if can grant lock to current transaction
  auto can_grant_lock = [&]() -> bool {
    bool can_grant = true;
    BUSTUB_ASSERT(lock_request_queue_ptr->upgrading_ == current_txn_id, "one and only one transaction is upgrading");
    for (auto &request_ptr : request_queue) {
      if (!request_ptr->granted_) {
        continue;
      }
      if (!AreLocksCompatible(lock_mode, request_ptr->lock_mode_)) {
        // lock_wanted is imcompatible with this lock
        can_grant = false;
        break;
      }
    }
    return can_grant;
  };

  auto grant_lock = [&]() -> void {
    // 1. find current_lock_request in block-queue
    BUSTUB_ASSERT(!request_queue.empty(), "request queue shold not be empty");
    txn_id_t current_txn_id = txn->GetTransactionId();
    auto current_request_iter = std::find_if(
        request_queue.begin(), request_queue.end(),
        [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
    BUSTUB_ASSERT(current_request_iter != request_queue.end(), "should have a lock request in block-queue");
    // 2. grant lock to transaction
    // 2.1. update lock-set in transaction
    // 2.1.1 get lock-set
    std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> txn_row_lock_set_ptr;
    switch (lock_mode) {
      case LockMode::INTENTION_SHARED:
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
      case LockMode::INTENTION_EXCLUSIVE:
        throw bustub::Exception("there should on be intension lock in row level");
        break;
      case LockMode::EXCLUSIVE:
        txn_row_lock_set_ptr = txn->GetExclusiveRowLockSet();
        break;
      case LockMode::SHARED:
        txn_row_lock_set_ptr = txn->GetSharedRowLockSet();
        break;
    }
    BUSTUB_ASSERT(txn_row_lock_set_ptr != nullptr, "error here");
    // 2.1.2 update lock-set
    (*txn_row_lock_set_ptr)[oid].insert(rid);
    // 2.2. update block-queue(in other word: current lock request)
    (*current_request_iter)->granted_ = true;
  };

  // 8.0 generate new lock request
  auto current_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  // 8.0 add current request to the end of queue
  request_queue.emplace_back(std::move(current_request));

  if (can_grant_lock()) {
    // 8.1 can grant lock to current transaction
    TransactionLatchGuard txn_lock(txn);
    grant_lock();
    lock_request_queue_ptr->upgrading_ = INVALID_TXN_ID;
    return true;
  }
  while (true) {
    // 8.2. cannot grant lock, wait until notified
    lock_request_queue_ptr->cv_.wait(block_queue_lock);
    // 8.3 notified
    // 惊群效应？
    {
      TransactionLatchGuard txn_lock(txn);
      if (txn->GetState() == TransactionState::ABORTED) {
        // TODO(SMLZ) delete from request-queue?
        txn_id_t current_txn_id = txn->GetTransactionId();
        auto current_request_iter = std::find_if(
            request_queue.begin(), request_queue.end(),
            [current_txn_id](const std::shared_ptr<LockRequest> &value) { return value->txn_id_ == current_txn_id; });
        request_queue.erase(current_request_iter);
        return false;
      }
      if (can_grant_lock()) {
        // 8.4 can grant lock to current transaction
        grant_lock();
        lock_request_queue_ptr->upgrading_ = INVALID_TXN_ID;
        return true;
      }
    }
    // 8.5 still cannot grant, keep waiting
  }
  return false;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  IsolationLevel iso_level = txn->GetIsolationLevel();
  TransactionState txn_state = txn->GetState();
  if (iso_level == IsolationLevel::REPEATABLE_READ) {
    if (txn_state == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "abort 10" << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }
  if (iso_level == IsolationLevel::READ_COMMITTED) {
    if (txn_state == TransactionState::SHRINKING) {
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        std::cout << "abort 11" << std::endl;
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    }
  }
  if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
    if (txn_state == TransactionState::GROWING) {
      if (lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        std::cout << "abort 12" << std::endl;
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
    }
    if (txn_state == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "abort 13" << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }
  // if (txn_state == TransactionState::ABORTED) {
  //   return false;
  // }
  return true;
}

auto LockManager::CanTxnTakeTableLock(Transaction *txn, LockMode lock_mode) -> bool {
  TransactionLatchGuard txn_lock(txn);
  return CanTxnTakeLock(txn, lock_mode);
}

auto LockManager::CanTxnTakeRowLock(Transaction *txn, LockMode lock_mode) -> bool {
  TransactionLatchGuard txn_lock(txn);
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    std::cout << "abort 14" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }
  return CanTxnTakeLock(txn, lock_mode);
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  TransactionLatchGuard txn_lock(txn);
  if (row_lock_mode == LockMode::EXCLUSIVE) {
    if (!(txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid))) {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "abort 15" << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  }
  if (row_lock_mode == LockMode::SHARED) {
    if (!(txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid) || txn->IsTableSharedLocked(oid) ||
          txn->IsTableIntentionSharedLocked(oid))) {
      txn->SetState(TransactionState::ABORTED);
      std::cout << "abort 16" << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  }
  return true;
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  return lock_compatibility_table_[static_cast<int>(l1)][static_cast<int>(l2)];
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == requested_lock_mode) {
    return true;
  }
  switch (curr_lock_mode) {
    case LockMode::INTENTION_SHARED:
      if (requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::EXCLUSIVE ||
          requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
          requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (requested_lock_mode == LockMode::EXCLUSIVE) {
        return true;
      }
    default:
      return false;
  }
}

auto LockManager::GetCurrentRowLockMode(Transaction *txn, const table_oid_t &oid, const RID &rid)
    -> std::optional<LockMode> {
  TransactionLatchGuard txn_lock(txn);
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    return LockMode::EXCLUSIVE;
  }
  if (txn->IsRowSharedLocked(oid, rid)) {
    return LockMode::SHARED;
  }
  return std::nullopt;
}

auto LockManager::GetCurrentTableLockMode(Transaction *txn, const table_oid_t &oid) -> std::optional<LockMode> {
  // lock needed!!
  TransactionLatchGuard txn_guard(txn);
  if (txn->IsTableExclusiveLocked(oid)) {
    return LockMode::EXCLUSIVE;
  }
  if (txn->IsTableSharedLocked(oid)) {
    return LockMode::SHARED;
  }
  if (txn->IsTableIntentionSharedLocked(oid)) {
    return LockMode::INTENTION_SHARED;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    return LockMode::INTENTION_EXCLUSIVE;
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    return LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  return std::nullopt;
}
}  // namespace bustub
