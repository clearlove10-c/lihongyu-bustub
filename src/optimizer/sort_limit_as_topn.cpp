#include <memory>
#include "common/macros.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    BUSTUB_ASSERT(optimized_plan->GetChildren().size() == 1, "error here");
    const auto &child_plan = optimized_plan->GetChildAt(0);
    const auto *current_limit_plan = dynamic_cast<const LimitPlanNode *>(optimized_plan.get());
    if (child_plan->GetType() == PlanType::Sort) {
      const auto *child_sort_plan = dynamic_cast<const SortPlanNode *>(child_plan.get());
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, optimized_plan->GetChildAt(0),
                                            child_sort_plan->GetOrderBy(), current_limit_plan->GetLimit());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
