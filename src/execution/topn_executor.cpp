#include "execution/executors/topn_executor.h"
#include <optional>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "execution/executors/init_check_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      container_(plan_->GetN(), plan_, *plan_->output_schema_) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  InitHeap();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (container_.Empty()) {
    return false;
  }
  *tuple = container_.PopTop();
  *rid = tuple->GetRid();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return container_.GetSize(); };

void TopNExecutor::InitHeap() {
  Tuple child_tuple;
  RID child_rid;
  container_.Clear();
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    container_.Insert(child_tuple);
  }
}

}  // namespace bustub
