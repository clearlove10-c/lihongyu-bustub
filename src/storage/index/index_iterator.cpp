/**
 * index_iterator.cpp
 */
#include <cassert>
#include <utility>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(ReadPageGuard leaf_guard, int index, BufferPoolManager *bpm)
    : current_leaf_guard_(std::move(leaf_guard)), current_index_(index), bpm_(bpm) {
  current_page_id_ = current_leaf_guard_.PageId();
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return static_cast<bool>(bpm_ == nullptr && current_index_ == 0); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto page = current_leaf_guard_.As<LeafPage>();
  return page->KVAt(current_index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto current_leaf_page = current_leaf_guard_.As<LeafPage>();
  // KeyType key = current_leaf_page->KeyAt(current_index_);
  // std::cout << "old iterator: index: " << current_index_ << " page_id: " << current_page_id_ << " key: " << key
  //           << std::endl;
  if (current_index_ == current_leaf_page->GetSize() - 1) {
    if (current_leaf_page->GetNextPageId() == INVALID_PAGE_ID) {
      current_index_ = 0;
      bpm_ = nullptr;
      current_page_id_ = INVALID_PAGE_ID;
      current_leaf_guard_.Drop();
      return *this;
    }
    current_index_ = 0;
    auto next_guard = bpm_->FetchPageRead(current_leaf_page->GetNextPageId());
    current_leaf_guard_ = std::move(next_guard);
    current_page_id_ = current_leaf_guard_.PageId();
    // current_leaf_page = current_leaf_guard_.As<LeafPage>();
  } else {
    ++current_index_;
  }
  // key = current_leaf_page->KeyAt(current_index_);
  // std::cout << "new iterator: index: " << current_index_ << " page_id: " << current_page_id_ << " key: " << key
  //           << std::endl;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
