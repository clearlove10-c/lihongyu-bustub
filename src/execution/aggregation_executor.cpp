//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <iostream>
#include <memory>
#include <optional>
#include <vector>

#include "common/macros.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_({plan->GetAggregates(), plan->GetAggregateTypes()}) {}

void AggregationExecutor::Init() {
  // 1. init child executor
  child_executor_->Init();
  // 2. init aggregation hash map
  InitAggregationHashTable();
  // 3. init iterator
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!aht_iterator_.has_value() || aht_iterator_.value() == aht_.End()) {
    // 1. iter reaches end
    return false;
  }
  // 1. get agg_key and agg_value from iterator
  AggregateKey agg_key = aht_iterator_->Key();
  AggregateValue agg_value = aht_iterator_->Val();
  // 2. add agg_value vector to the end of agg_key vector
  std::vector<Value> values = agg_key.group_bys_;
  values.insert(values.end(), agg_value.aggregates_.begin(), agg_value.aggregates_.end());
  // 3. generate result tuple from {agg_key + agg_value}
  *tuple = Tuple(values, &GetOutputSchema());
  *rid = tuple->GetRid();
  // 4. add iterator
  ++(*aht_iterator_);
  return true;
}

void AggregationExecutor::InitAggregationHashTable() {
  Tuple child_tuple;
  RID child_rid;
  // 0. clear old hash map
  aht_.Clear();
  AggregateKey agg_key;
  AggregateValue agg_value;
  // 1. get child_tuple
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // 2. generate agg_key and agg_value
    agg_key = MakeAggregateKey(&child_tuple);
    agg_value = MakeAggregateValue(&child_tuple);
    // 3. insert kv into agg hashmap
    aht_.InsertCombine(agg_key, agg_value);
  }
  if (aht_.IsEmpty() && plan_->group_bys_.empty()) {
    // 1. empty table with no group by: insert default value for empty key
    aht_.InsertDefault();
  }
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
