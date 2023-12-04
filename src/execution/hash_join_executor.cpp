//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstdint>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/macros.h"
#include "storage/table/tuple.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // 1. init child executor
  left_executor_->Init();
  right_executor_->Init();
  // 2. init hash map
  InitHashMap();
}

// TODO(SMLZ) 改成map中存右子表，为了支持left join
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;

  if (stashed_right_tuples_.empty()) {
    bool matched = false;
    // no stashed right tuple, need to aquire tuple from left that matches in hash map
    // 1. get stashed right tuples and corresbonding left tuple
    while (left_executor_->Next(&left_tuple, &left_rid)) {
      HashJoinKey left_key = MakeLeftHashKey(&left_tuple);
      HashJoinValue match_tuples = map_.Get(left_key);
      if (!match_tuples.tuples_.empty()) {
        // match success
        // move here!!! use after move is invalid!!!
        stashed_right_tuples_ = std::move(match_tuples.tuples_);
        stashed_left_tuple_ = left_tuple;
        matched = true;
        break;
      }
      // match fail
      if (plan_->GetJoinType() == JoinType::LEFT) {
        *tuple = GenerateTupleByDefault(left_tuple);
        *rid = tuple->GetRid();
        return true;
      }
    }
    if (!matched) {
      return false;
    }
  }

  BUSTUB_ASSERT(!stashed_right_tuples_.empty(), "error here");
  // 2. pop to right tuple
  right_tuple = stashed_right_tuples_.back();
  stashed_right_tuples_.pop_back();
  // 3. assgin left tuple
  left_tuple = stashed_left_tuple_;
  // 3. generate result values
  *tuple = GenerateTupleFromTuples(left_tuple, right_tuple);
  *rid = tuple->GetRid();
  return true;
}

void HashJoinExecutor::InitHashMap() {
  Tuple child_tuple;
  RID child_rid;
  // 0. clear old hash map
  map_.Clear();
  HashJoinKey hash_key;
  // 1. get child_tuple
  while (right_executor_->Next(&child_tuple, &child_rid)) {
    // 2. generate hash_key
    hash_key = MakeRightHashKey(&child_tuple);
    // 3. insert kv into hashmap
    map_.Insert(hash_key, child_tuple);
  }
}

}  // namespace bustub
