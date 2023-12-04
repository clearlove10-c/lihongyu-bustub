#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <cstddef>
#include <utility>
#include "binder/bound_order_by.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  InitTupleSet();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (current_cursor_ >= set_.size()) {
    return false;
  }
  *tuple = set_[current_cursor_];
  *rid = tuple->GetRid();
  ++current_cursor_;
  return true;
}

void SortExecutor::InitTupleSet() {
  set_.clear();
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    set_.emplace_back(std::move(child_tuple));
  }
  const Schema schema = GetOutputSchema();
  std::sort(set_.begin(), set_.end(), [&](const Tuple &left, const Tuple &right) -> bool {
    Value left_value;
    Value right_value;
    CmpBool cmp_result;
    for (const auto &order_rule : plan_->GetOrderBy()) {
      left_value = order_rule.second->Evaluate(&left, schema);
      right_value = order_rule.second->Evaluate(&right, schema);
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
  });
}

}  // namespace bustub
