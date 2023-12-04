//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {
struct HashJoinKey {
  std::vector<Value> cols_;

  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.cols_.size(); i++) {
      if (cols_[i].CompareEquals(other.cols_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

struct HashJoinValue {
  std::vector<Tuple> tuples_;
};
}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct ::std::hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.cols_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std
namespace bustub {

class HashJoinMap {
 public:
  HashJoinMap() = default;
  ~HashJoinMap() = default;

  void Insert(const HashJoinKey &key, const Tuple &tuple) {
    if (map_.count(key) == 0) {
      map_.insert({key, {{tuple}}});
    } else {
      map_[key].tuples_.emplace_back(tuple);
    }
  }

  auto Get(const HashJoinKey &key) const -> HashJoinValue {
    if (map_.count(key) == 0) {
      return {{}};
    }
    return map_.at(key);
  }

  void Clear() { map_.clear(); }

 private:
  std::unordered_map<HashJoinKey, HashJoinValue> map_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  void InitHashMap();

  auto MakeLeftHashKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, left_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  auto MakeRightHashKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, right_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  inline auto GenerateTupleFromTuples(const Tuple &left, const Tuple &right) -> Tuple {
    std::vector<Value> values;
    values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                   right_executor_->GetOutputSchema().GetColumnCount());
    for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.push_back(left.GetValue(&left_executor_->GetOutputSchema(), i));
    }
    for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.push_back(right.GetValue(&right_executor_->GetOutputSchema(), i));
    }
    return {values, &GetOutputSchema()};
  }

  inline auto GenerateTupleByDefault(const Tuple &left) -> Tuple {
    std::vector<Value> values;
    values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                   right_executor_->GetOutputSchema().GetColumnCount());
    for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.push_back(left.GetValue(&left_executor_->GetOutputSchema(), i));
    }
    for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
    }
    return {values, &GetOutputSchema()};
  }

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_executor_;

  std::unique_ptr<AbstractExecutor> right_executor_;

  HashJoinMap map_{};

  std::vector<Tuple> stashed_right_tuples_{};

  Tuple stashed_left_tuple_;
};

}  // namespace bustub
