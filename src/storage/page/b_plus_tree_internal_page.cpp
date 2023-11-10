//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  // TODO(SMLZ) ?
  SetSize(0);
  SetMaxSize(max_size);
  // when to resize the array_?
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  auto mapping_type = array_[index];
  return mapping_type.first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index < GetSize(), "error here");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKV(int index, const KeyType &key, const ValueType &value) {
  BUSTUB_ASSERT(index <= GetSize(), "error here");
  IncreaseSize(1);
  for (int i = GetSize() - 1; i > index; --i) {
    array_[i] = array_[i - 1];
  }
  array_[index] = {key, value};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKVOverflow(int index, const KeyType &key, const ValueType &value) {
  for (int i = index + 1; i < GetSize(); ++i) {
    array_[i] = array_[i - 1];
  }
  array_[index] = {key, value};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteKV(int index) {
  BUSTUB_ASSERT(index < GetSize(), "error here");
  DecreaseSize(1);
  for (int i = index; i < GetSize(); ++i) {
    array_[i] = array_[i + 1];
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KVAt(int index) const -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindKeyPosition(KeyType key, const KeyComparator &cmp) -> int {
  int left = 1;
  int right = GetSize();
  while (left < right) {
    int mid = left + (right - left) / 2;
    if (cmp(array_[mid].first, key) != 1) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left - 1;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
