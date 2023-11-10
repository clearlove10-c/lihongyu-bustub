//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator() = default;
  explicit IndexIterator(ReadPageGuard leaf_guard, int index, BufferPoolManager *bpm);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return this->current_index_ == itr.current_index_ && this->current_page_id_ == itr.current_page_id_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return this->current_index_ != itr.current_index_ || this->current_page_id_ != itr.current_page_id_;
  }

 private:
  // add your own private member variables here
  // TODO(SMLZ) raw_ptr here
  // LeafPage *current_leaf_page_ = nullptr;
  ReadPageGuard current_leaf_guard_;
  int current_index_ = 0;
  BufferPoolManager *bpm_ = nullptr;
  page_id_t current_page_id_ = INVALID_PAGE_ID;
};

}  // namespace bustub
