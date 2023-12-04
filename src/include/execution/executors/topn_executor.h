//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct Compare {
  Compare(const TopNPlanNode *plan, const Schema &schema) : plan_(plan), schema_(schema) {}
  Compare(Compare &&other) = default;
  Compare(const Compare &other) = default;
  auto operator=(Compare &&other) -> Compare & = delete;
  auto operator=(const Compare &other) -> Compare & = delete;

  auto operator()(const Tuple &left, const Tuple &right) const -> bool {
    Value left_value;
    Value right_value;
    CmpBool cmp_result;
    for (const auto &order_rule : plan_->GetOrderBy()) {
      left_value = order_rule.second->Evaluate(&left, schema_);
      right_value = order_rule.second->Evaluate(&right, schema_);
      switch (order_rule.first) {
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          cmp_result = left_value.CompareLessThan(right_value);
          switch (cmp_result) {
            case CmpBool::CmpTrue:
              return true;
            case CmpBool::CmpFalse:
              if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
                continue;
              } else {
                return false;
              }
            case CmpBool::CmpNull:
              BUSTUB_ASSERT(cmp_result != CmpBool::CmpNull, "error here");
              break;
          }
          break;
        case OrderByType::DESC:
          cmp_result = left_value.CompareLessThan(right_value);
          switch (cmp_result) {
            case CmpBool::CmpTrue:
              return false;
            case CmpBool::CmpFalse:
              if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
                continue;
              } else {
                return true;
              }
            case CmpBool::CmpNull:
              BUSTUB_ASSERT(cmp_result != CmpBool::CmpNull, "error here");
              break;
          }
          break;
        case OrderByType::INVALID:
          return true;
      }
    }
    BUSTUB_ASSERT(false, "can't reach here: no same tuple");
    return true;
  }
  const TopNPlanNode *plan_;
  const Schema &schema_;
};

class TopNContainer {
 public:
  TopNContainer(size_t limit, const TopNPlanNode *plan, const Schema &schema)
      : limit_(limit), cmp_({plan, schema}), tuple_set_(std::set<Tuple, Compare>(cmp_)) {}

  void Insert(const Tuple &tuple) {
    if (tuple_set_.size() < limit_) {
      tuple_set_.insert(tuple);
    } else {
      auto min_element = *tuple_set_.rbegin();
      if (cmp_(tuple, min_element)) {
        tuple_set_.erase(--tuple_set_.end());
        tuple_set_.insert(tuple);
      }
    }
  }

  auto ToString() const -> std::string {
    std::stringstream os;
    for (const auto &element : tuple_set_) {
      os << element.ToString(&cmp_.schema_) << " ";
    }
    os << std::endl;
    return os.str();
  }

  auto PopTop() -> Tuple {
    auto max_element = *tuple_set_.begin();
    tuple_set_.erase(tuple_set_.begin());
    return max_element;
  }

  void Clear() { tuple_set_.clear(); }

  auto GetSize() -> size_t { return tuple_set_.size(); }

  auto Empty() const -> bool { return tuple_set_.empty(); }

 private:
  size_t limit_;
  const Compare cmp_;
  std::set<Tuple, Compare> tuple_set_;
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  void InitHeap();

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  TopNContainer container_;
};
}  // namespace bustub
