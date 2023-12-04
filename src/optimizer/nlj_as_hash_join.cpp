#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stack>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    // annonce left key expression ans right key expression
    std::vector<AbstractExpressionRef> left_key_expr;
    std::vector<AbstractExpressionRef> right_key_expr;

    auto add_key_expr_from_compare = [&](const ComparisonExpression *expr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            // Now it's in form of <column_expr> = <column_expr>
            if (left_expr->GetTupleIdx() == 1) {
              // left expr belongs to right table
              std::cout << "SWAP NEEDED" << std::endl;
              std::swap(left_expr, right_expr);
            }
            left_key_expr.emplace_back(
                std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
            right_key_expr.emplace_back(
                std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
          }
        }
      }
    };

    std::stack<AbstractExpression *> expr_stack;
    expr_stack.push(nlj_plan.Predicate().get());
    while (!expr_stack.empty()) {
      AbstractExpression *origin_expr = expr_stack.top();
      expr_stack.pop();
      if (const auto *expr = dynamic_cast<const LogicExpression *>(origin_expr); expr != nullptr) {
        // is logic expression, add children to stack
        BUSTUB_ASSERT(expr->children_.size() == 2, "error here");
        expr_stack.push(expr->GetChildAt(0).get());
        expr_stack.push(expr->GetChildAt(1).get());
      } else if (const auto *expr = dynamic_cast<const ComparisonExpression *>(origin_expr); expr != nullptr) {
        // is compare expression
        add_key_expr_from_compare(expr);
      }
    }
    std::cout << std::endl;
    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                              left_key_expr, right_key_expr, nlj_plan.join_type_);
  }
  return plan;
}

}  // namespace bustub
