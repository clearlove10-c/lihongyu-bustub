//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <optional>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // 1. init child executor
  left_executor_->Init();
  right_executor_->Init();
  // 2. init current left tuple
  Tuple left_tuple{};
  RID left_rid;
  if (!left_executor_->Next(&left_tuple, &left_rid)) {
    is_empty_table_ = true;
    return;
  }
  current_left_tuple_ = left_tuple;
  // reset stop flag
  reaches_end_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple{};
  RID left_rid;
  Tuple right_tuple{};
  RID right_rid;
  bool status = true;
  // 0. get predicate expression
  auto predicate_expr = plan_->Predicate();
  while (!reaches_end_) {
    // inner loop
    // 1. init left tuple
    left_tuple = current_left_tuple_;
    // 2. init right tuple
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      // outer loop
      // 4. evaluate
      auto evaluate_result_value = predicate_expr->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                                &right_tuple, right_executor_->GetOutputSchema());
      bool evaluate_result = evaluate_result_value.IsNull() ? false : evaluate_result_value.GetAs<bool>();
      if (evaluate_result) {
        // 5. evaluate ok
        left_jion_flag_ = true;
        // 6. generate values;
        std::vector<Value> values{};
        values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                       right_executor_->GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        // 7. generate tuple and rid
        *tuple = Tuple{values, &GetOutputSchema()};
        *rid = tuple->GetRid();
        return true;
      }
      // 5. evaluate fail, continue aquiring next right tuple
    }
    // finish right scan
    if (!left_jion_flag_ && plan_->GetJoinType() == JoinType::LEFT) {
      // left scan and no matching for current left tuple
      // 3. generate values
      std::vector<Value> values{};
      values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                     right_executor_->GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      // 4. generate tuple and rid
      *tuple = Tuple{values, &GetOutputSchema()};
      *rid = tuple->GetRid();

      // 5. aquire for next left tuple and update it
      status = left_executor_->Next(&left_tuple, &left_rid);
      if (!status) {
        // 6. reaches end
        reaches_end_ = true;
      }
      current_left_tuple_ = left_tuple;
      // 6. restart right executor
      right_executor_->Init();
      // 7. reset left jion flag
      left_jion_flag_ = false;
      // 8. return defalut value for current left tuple
      return true;
    }
    // 3. aquire for next left tuple and update it
    status = left_executor_->Next(&left_tuple, &left_rid);
    if (!status) {
      // 4. reaches end
      reaches_end_ = true;
      return false;
    }
    current_left_tuple_ = left_tuple;
    // 4. restart right executor
    right_executor_->Init();
    left_jion_flag_ = false;
  }
  return false;
}

}  // namespace bustub
