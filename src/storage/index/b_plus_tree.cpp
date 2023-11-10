#include <shared_mutex>
#include <sstream>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
  // for test use
  std::cout << "leaf_max_size: " << leaf_max_size << " internal_max_size: " << internal_max_size << std::endl;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // // for test use
  // std::cout << "my_get key: " << key << " count: " << count_ << std::endl;
  // count_++;
  // std::lock_guard<std::mutex> lock(mutex_);
  // Declaration of context instance.
  Context ctx;
  GetLeafPageRead(key, ctx);
  if (ctx.is_tree_empty_) {
    return false;
  }
  auto leaf_page = ctx.current_read_guard_.As<LeafPage>();
  for (int i = 0; i < leaf_page->GetSize(); ++i) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      result->emplace_back(leaf_page->ValueAt(i));
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // for test use
  // mutex_.lock();
  // std::cout << "my_insert key: " << key << " count: " << count_ << std::endl;
  // std::cout << this->DrawBPlusTree() << std::endl;
  // count_++;
  // mutex_.unlock();
  std::shared_lock<std::shared_mutex> lock(root_mutex_);

  // Declaration of context instance.
  Context ctx;

  // 1. get leaf page
  GetLeafPageInsert(key, ctx);
  if (ctx.is_tree_empty_) {
    // 2. create tree if it's empty
    page_id_t root_id_new;
    auto root_guard = bpm_->NewPageGuarded(&root_id_new);
    auto root_page_new = root_guard.AsMut<LeafPage>();
    root_page_new->Init(leaf_max_size_);
    root_page_new->InsertKV(0, key, value);
    ctx.header_write_guard_.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = root_id_new;
    return true;
  }
  WritePageGuard leaf_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  page_id_t leaf_page_id = leaf_guard.PageId();
  auto leaf_page = leaf_guard.AsMut<LeafPage>();

  // 2. find index (index is where to insert the new kv)
  int index = leaf_page->FindKeyPosition(key, comparator_);
  if (comparator_(leaf_page->KeyAt(index), key) == 0) {
    return false;
  }

  if (leaf_page->IsSafeForInsert()) {
    // 3. simply insert
    // hold lock:
    // root != leaf :current(leaf).W
    // root == leaf :header.W, current.W
    leaf_page->InsertKV(index, key, value);
    return true;
  }

  // insert then split
  // 3. insert
  leaf_page->InsertKV(index, key, value);
  // 4. split
  // 4.1. generate new leaf page
  page_id_t brother_leaf_page_id;
  auto brother_leaf_guard = bpm_->NewPageGuarded(&brother_leaf_page_id);
  auto brother_leaf_page = brother_leaf_guard.AsMut<LeafPage>();
  brother_leaf_page->Init(leaf_max_size_);
  // 4.2. update linked list
  brother_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(brother_leaf_page_id);
  // 4.3. move kvs to new page
  int min_size = leaf_page->GetMinSize();
  int max_size = leaf_page->GetMaxSize();
  int move_count = max_size - min_size;
  for (int i = 0; i < move_count; ++i) {
    brother_leaf_page->InsertKV(i, leaf_page->KeyAt(min_size + i), leaf_page->ValueAt(min_size + i));
  }
  // 4.4. delete those kvs from old page
  BUSTUB_ASSERT(min_size > 0, "erroe here");
  leaf_page->SetSize(min_size);

  // ---------------internal level------------------------
  // InternalPage *current_page;
  InternalPage *new_page;
  InternalPage *parent_page;
  WritePageGuard current_guard;
  BasicPageGuard new_guard;
  WritePageGuard parent_guard;
  // page_id_t current_page_id;
  page_id_t new_page_id;
  page_id_t parent_page_id;
  page_id_t right_child_page_id = brother_leaf_page_id;
  page_id_t left_child_page_id = leaf_page_id;
  KeyType right_child_key = brother_leaf_page->KeyAt(0);
  KeyType left_child_key = leaf_page->KeyAt(0);
  // 4.5 update parents
  // new brother is always on the right side

  // ctx.write_set_:
  // root == leaf: header; root/current/leaf(poped)
  // root != leaf: lowest safe page ... leaf(poped)
  // when root is safe we still have header.W and root.W
  while (!ctx.write_set_.empty()) {
    // 4.6. get parent page
    parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    parent_page = parent_guard.AsMut<InternalPage>();
    parent_page_id = parent_guard.PageId();
    // 4.7. get index to insert
    // index_set is index travels through
    index = ctx.index_set_.back() + 1;
    ctx.index_set_.pop_back();

    if (parent_page->IsSafeForInsert()) {
      // parent do not need to split
      // 4.8. insert into parent

      parent_page->InsertKV(index, right_child_key, right_child_page_id);
      return true;
    }
    // parent need to split
    // split then insert
    // TODO(SMLZ) use guard->page_id instead of declared page_id?
    // 4.8. generate new parent page
    new_guard = bpm_->NewPageGuarded(&new_page_id);
    new_page = new_guard.AsMut<InternalPage>();
    new_page->Init(internal_max_size_);
    min_size = parent_page->GetMinSize();
    max_size = parent_page->GetMaxSize();
    if (index >= min_size) {
      // insert into new page
      // 4.9. move kvs to new page and insert to new page
      move_count = max_size - min_size;
      for (int i = 0; i < move_count; ++i) {
        new_page->InsertKV(i, parent_page->KeyAt(min_size + i), parent_page->ValueAt(min_size + i));
      }
      new_page->InsertKV(index - min_size, right_child_key, right_child_page_id);
    } else {
      // insert into old page
      // 4.9. move kvs to new page and insert to old page
      move_count = max_size - min_size + 1;
      for (int i = 0; i < move_count; ++i) {
        new_page->InsertKV(i, parent_page->KeyAt(min_size - 1 + i), parent_page->ValueAt(min_size - 1 + i));
      }
      parent_page->InsertKVOverflow(index, right_child_key, right_child_page_id);
    }
    BUSTUB_ASSERT(min_size > 0, "erroe here");
    parent_page->SetSize(min_size);

    right_child_key = new_page->KeyAt(0);
    right_child_page_id = new_page_id;
    left_child_key = parent_page->KeyAt(0);
    left_child_page_id = parent_page_id;
  }
  // root need split(need update root_id)
  // root == leaf && root is unsafe
  // root != leaf &&
  // need to create a new root(page type: internal page)
  // it's always binary when creating a new root
  page_id_t root_id_new;
  auto root_guard = bpm_->NewPageGuarded(&root_id_new);
  auto root_page_new = root_guard.AsMut<InternalPage>();
  root_page_new->Init(internal_max_size_);
  root_page_new->InsertKV(0, left_child_key, left_child_page_id);
  root_page_new->InsertKV(1, right_child_key, right_child_page_id);
  auto header_guard = std::move(ctx.header_write_guard_);
  header_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = root_id_new;
  return true;
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // for test use
  // mutex_.lock();
  // std::cout << "my_delete key: " << key << " count: " << count_ << std::endl;
  // std::cout << this->DrawBPlusTree() << std::endl;
  // mutex_.unlock();
  // count_++;
  // std::lock_guard<std::mutex> lock(mutex_);
  std::unique_lock<std::shared_mutex> lock(root_mutex_);
  // Declaration of context instance.
  Context ctx;

  // 1. get leaf page
  GetLeafPageDelete(key, ctx);
  if (ctx.is_tree_empty_) {
    // 2. return if it's empty
    return;
  }
  WritePageGuard current_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  // page_id_t leaf_page_id = current_guard.PageId();
  auto leaf_page = current_guard.AsMut<LeafPage>();

  // 2. find index (index is where to delete the new kv)
  int index = leaf_page->FindKeyPosition(key, comparator_);
  if (index >= leaf_page->GetSize() || comparator_(leaf_page->KeyAt(index), key) != 0) {
    return;
  }

  if (leaf_page->IsSafeForDelete() || (ctx.IsRootLeaf() && leaf_page->GetSize() >= 2)) {
    // 3. simply delete when leaf is safe or leaf is root with more than one value
    leaf_page->DeleteKV(index);
    return;
  }

  if (ctx.IsRootLeaf() && leaf_page->GetSize() == 1) {
    // TODO(SMLZ) delete root
    ctx.header_write_guard_.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
    return;
  }

  // leaf level---------------------------------------------------------------------------------------
  // 1. get parent page
  InternalPage *parent_page;
  WritePageGuard parent_guard;
  parent_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  parent_page = parent_guard.AsMut<InternalPage>();
  // 2. find brother page
  bool is_right_brother;
  WritePageGuard brother_guard;
  LeafPage *brother_page_leaf;
  brother_guard = bpm_->FetchPageWrite(GetBrotherPageID(leaf_page, parent_page, is_right_brother));
  brother_page_leaf = brother_guard.AsMut<LeafPage>();
  // 2.5 get index of both current and brother
  auto current_index = GetIndexOfChild(leaf_page->KeyAt(leaf_page->GetSize() - 1), parent_page);
  auto brother_index = GetIndexOfChild(brother_page_leaf->KeyAt(brother_page_leaf->GetSize() - 1), parent_page);
  // BUSTUB_ASSERT(abs(current_index - brother_index) == 1, "error here");
  // 3. delete current page
  leaf_page->DeleteKV(index);
  // 4. borrow or merge
  if (brother_page_leaf->IsSafeForDelete()) {
    // borrow
    if (is_right_brother) {
      // borrow from right
      // 5. insert to current page
      leaf_page->InsertKV(leaf_page->GetSize(), brother_page_leaf->KeyAt(0), brother_page_leaf->ValueAt(0));
      // 6. delete in brother page
      brother_page_leaf->DeleteKV(0);
      // 7. update key of brother page in parent page
      int brother_index = GetIndexOfChild(brother_page_leaf->KeyAt(brother_page_leaf->GetSize() - 1), parent_page);
      parent_page->SetKeyAt(brother_index, brother_page_leaf->KeyAt(0));
      // 8. finished (parent's don't need to change)
      return;
    }
    // borrow from left
    // 5. insert to current page
    leaf_page->InsertKV(0, brother_page_leaf->KeyAt(brother_page_leaf->GetSize() - 1),
                        brother_page_leaf->ValueAt(brother_page_leaf->GetSize() - 1));
    // 6. delete in brother page
    brother_page_leaf->DeleteKV(brother_page_leaf->GetSize() - 1);
    // 7. update key of current page in parent page
    int current_index = GetIndexOfChild(leaf_page->KeyAt(leaf_page->GetSize() - 1), parent_page);
    parent_page->SetKeyAt(current_index, leaf_page->KeyAt(0));
    // 8. finished (parent's don't need to change)
    return;
  }
  // merge (merge right to left)
  if (is_right_brother) {
    // brother merge to current
    // 5. move kv from brother to current
    for (int i = 0; i < brother_page_leaf->GetSize(); ++i) {
      leaf_page->InsertKV(leaf_page->GetSize(), brother_page_leaf->KeyAt(i), brother_page_leaf->ValueAt(i));
    }
    // 6. set delete-index to index of brother page (delete kv in parent in next loop)
    index = brother_index;
    // 8. update linked list
    leaf_page->SetNextPageId(brother_page_leaf->GetNextPageId());
    // 7. TODO(SMLZ) delete brother
    brother_guard.Drop();
    BUSTUB_ASSERT(leaf_page->GetSize() > 0, "erroe here");
  } else {
    // current merge to brother
    // 5. move kv from current to brother
    for (int i = 0; i < leaf_page->GetSize(); ++i) {
      brother_page_leaf->InsertKV(brother_page_leaf->GetSize(), leaf_page->KeyAt(i), leaf_page->ValueAt(i));
    }
    // 6. set delete-index to index of current page (delete kv in parent in next loop)
    index = current_index;
    // 7. TODO(SMLZ) delete current

    // 9. update linked list
    brother_page_leaf->SetNextPageId(leaf_page->GetNextPageId());
    // 8. reset current to brother
    current_guard = std::move(brother_guard);
  }

  // intrenal page level------------------------------------------------------------------------------------------
  InternalPage *current_page;
  InternalPage *brother_page;
  while (!parent_page->IsSafeForDelete() && !ctx.write_set_.empty()) {
    // 0. update current page
    current_guard = std::move(parent_guard);
    current_page = parent_page;
    // 1. get parent page
    parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    parent_page = parent_guard.AsMut<InternalPage>();
    // 2. find brother page
    brother_guard = std::move(bpm_->FetchPageWrite(GetBrotherPageID(current_page, parent_page, is_right_brother)));
    brother_page = brother_guard.AsMut<InternalPage>();
    // 2.5 get index of both current and brother
    auto current_index = GetIndexOfChild(current_page->KeyAt(current_page->GetSize() - 1), parent_page);
    auto brother_index = GetIndexOfChild(brother_page->KeyAt(brother_page->GetSize() - 1), parent_page);
    BUSTUB_ASSERT(abs(brother_index - current_index) == 1, "error here");
    // 3. delete in current page
    current_page->DeleteKV(index);
    // 4. borrow of merge
    if (brother_page->IsSafeForDelete()) {
      // borrow
      if (is_right_brother) {
        // borrow from right
        // 5. insert to current page
        current_page->InsertKV(current_page->GetSize(), brother_page->KeyAt(0), brother_page->ValueAt(0));
        // 6. delete in brother page
        brother_page->DeleteKV(0);
        // 7. update key of brother page in parent page
        parent_page->SetKeyAt(brother_index, brother_page->KeyAt(0));
        // 8. finished (parent's don't need to change)
        return;
      }
      // borrow from left
      // 5. insert to current page
      current_page->InsertKV(0, brother_page->KeyAt(brother_page->GetSize() - 1),
                             brother_page->ValueAt(brother_page->GetSize() - 1));
      // 6. delete in brother page
      brother_page->DeleteKV(brother_page->GetSize() - 1);
      // 7. update key of current page in parent page
      parent_page->SetKeyAt(current_index, current_page->KeyAt(0));
      // 8. finished (parent's don't need to change)
      return;
    }
    // merge (merge right to left)
    if (is_right_brother) {
      // brother merge to current
      // 5. fill the first key of right(brother page)
      index = brother_index;
      brother_page->SetKeyAt(0, parent_page->KeyAt(index));
      // 6. move kv from brother to current
      for (int i = 0; i < brother_page->GetSize(); ++i) {
        current_page->InsertKV(current_page->GetSize(), brother_page->KeyAt(i), brother_page->ValueAt(i));
      }
      // 7. set delete-index to index of brother page (delete kv in parent in next loop)
      // already done in 5
      // 8. TODO(SMLZ) delete brother
    } else {
      // current merge to brother
      // 5. fill the first key of right(current page)
      index = current_index;
      current_page->SetKeyAt(0, parent_page->KeyAt(index));
      auto test_temp = current_page->KVAt(0);
      (void)test_temp;
      // 6. move kv from current to brother
      for (int i = 0; i < current_page->GetSize(); ++i) {
        brother_page->InsertKV(brother_page->GetSize(), current_page->KeyAt(i), current_page->ValueAt(i));
      }
      // 7. set delete-index to index of current page (delete kv in parent in next loop)
      // already done in 5
      // 8. TODO(SMLZ) delete current
      // 9. reset current to brother
      current_guard = std::move(brother_guard);
    }
  }
  if (!ctx.is_root_safe_ && parent_page->GetSize() == 2) {
    // update root
    UpdateRootPageId(current_guard.PageId(), ctx.header_write_guard_);
    // std::cout << "update root: " << key << " count: " << count_ << std::endl;
    count_++;
  } else {
    // delete in parent
    parent_page->DeleteKV(index);
  }
  // if (parent_guard.PageId() == GetRootPageId()) {
  //   // update root
  //   UpdateRootPageId(current_guard.PageId());
  // }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto current_read_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = current_read_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  page_id_t root_page_id = header_page->root_page_id_;
  auto guard = bpm_->FetchPageRead(root_page_id);
  auto previous_read_guard = std::move(current_read_guard);
  current_read_guard = std::move(guard);
  auto current_page = current_read_guard.As<BPlusTreePage>();
  while (!current_page->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(const_cast<BPlusTreePage *>(current_page));
    guard = bpm_->FetchPageRead(internal->ValueAt(0));
    previous_read_guard = std::move(current_read_guard);
    current_read_guard = std::move(guard);
    current_page = current_read_guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(std::move(current_read_guard), 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  GetLeafPageRead(key, ctx);
  if (ctx.is_tree_empty_) {
    return INDEXITERATOR_TYPE();
  }
  auto leaf_page = ctx.current_read_guard_.As<LeafPage>();
  for (int i = 0; i < leaf_page->GetSize(); ++i) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      return INDEXITERATOR_TYPE(std::move(ctx.current_read_guard_), i, bpm_);
    }
  }
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
// TODO(SMLZ) like std iterator, pointer to elem after the last one
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  return header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageWrite(const KeyType &key, Context &ctx) -> LeafPage * {
  // (TODO)SMLZ: need check here?
  if (IsEmpty()) {
    return nullptr;
  }
  // use header_page to get root_page_id
  ctx.root_page_id_ = GetRootPageId();
  auto guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  ctx.current_page_id_ = guard.PageId();
  auto it = guard.AsMut<BPlusTreePage>();
  ctx.write_set_.emplace_back(std::move(guard));
  while (!it->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(it);
    // find child entry
    // TODO(SMLZ) change to binary search
    int index = 1;
    for (; index < internal->GetSize(); ++index) {
      if (comparator_(internal->KeyAt(index), key) == 1) {
        break;
      }
    }
    --index;
    // TODO(SMLZ) ?
    guard = std::move(bpm_->FetchPageWrite(internal->ValueAt(index)));
    it = guard.AsMut<BPlusTreePage>();
    ctx.current_page_id_ = guard.PageId();
    // 默认调用拷贝语义，需要强制转化为右值引用
    ctx.write_set_.emplace_back(std::move(guard));
  }
  return reinterpret_cast<LeafPage *>(it);
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::GetLeafPageRead(const KeyType &key, Context &ctx) -> LeafPage * {
//   // (TODO)SMLZ: need check here?
//   if (IsEmpty()) {
//     return nullptr;
//   }
//   // use header_page to get root_page_id
//   ctx.root_page_id_ = GetRootPageId();
//   auto guard = bpm_->FetchPageRead(ctx.root_page_id_);
//   ctx.current_page_id_ = guard.PageId();
//   auto it = guard.As<BPlusTreePage>();
//   ctx.current_read_guard_ = std::move(guard);
//   while (!it->IsLeafPage()) {
//     auto internal = reinterpret_cast<InternalPage *>(const_cast<BPlusTreePage *>(it));
//     // find child entry
//     // TODO(SMLZ) change to binary search
//     int index = 1;
//     for (; index < internal->GetSize(); ++index) {
//       if (comparator_(internal->KeyAt(index), key) == 1) {
//         break;
//       }
//     }
//     --index;
//     // aquire r_lock for next node
//     guard = std::move(bpm_->FetchPageRead(internal->ValueAt(index)));
//     it = guard.As<BPlusTreePage>();
//     ctx.current_page_id_ = guard.PageId();
//     // release r_lock for previous node
//     ctx.current_read_guard_ = std::move(guard);
//   }
//   return reinterpret_cast<LeafPage *>(const_cast<BPlusTreePage *>(it));
// }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageOptimistic(const KeyType &key, Context &ctx) -> LeafPage * {
  // use header_page to get root_page_id
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  ctx.root_page_id_ = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  // 1. get root_page
  auto guard = bpm_->FetchPageRead(ctx.root_page_id_);
  // 2. init current_page and current_guard as root
  ctx.current_page_id_ = guard.PageId();
  auto current_page = guard.As<BPlusTreePage>();
  ctx.current_read_guard_ = std::move(guard);
  if (current_page->IsLeafPage()) {
    // root is leaf
    // 3. upgrade read lock to write lock
    // no need to worry abt updates of current look
    ctx.current_read_guard_.Drop();
    ctx.current_write_guard_ = bpm_->FetchPageWrite(ctx.current_page_id_);
    header_guard.Drop();
    return reinterpret_cast<LeafPage *>(const_cast<BPlusTreePage *>(current_page));
  }
  // root = current != leaf
  header_guard.Drop();
  while (true) {
    auto internal = reinterpret_cast<InternalPage *>(const_cast<BPlusTreePage *>(current_page));
    // 3. find child entry
    // TODO(SMLZ) change to binary search
    int index = 1;
    for (; index < internal->GetSize(); ++index) {
      if (comparator_(internal->KeyAt(index), key) == 1) {
        break;
      }
    }
    --index;
    // 4. aquire r_lock for next node
    guard = std::move(bpm_->FetchPageRead(internal->ValueAt(index)));
    current_page = guard.As<BPlusTreePage>();
    ctx.current_page_id_ = guard.PageId();
    if (current_page->IsLeafPage()) {
      // 5. aquire write lock for leaf (upgrade)
      // because  have the patent's read lock, no need to worry other thread update leaf of parent
      guard.Drop();
      ctx.current_write_guard_ = bpm_->FetchPageWrite(ctx.current_page_id_);
      current_page = ctx.current_write_guard_.AsMut<BPlusTreePage>();
      break;
    }
    // 5. release r_lock for previous node
    ctx.current_read_guard_ = std::move(guard);
  }
  // 6. Drop parent's read lock
  ctx.current_read_guard_.Drop();
  return reinterpret_cast<LeafPage *>(const_cast<BPlusTreePage *>(current_page));
}

// auto BPLUSTREE_TYPE::GetLeafPageOptimistic(const KeyType &key, Context &ctx) -> LeafPage * {
//   // use header_page to get root_page_id
//   // ctx.root_page_id_ = GetRootPageId();
//   auto header_guard = bpm_->FetchPageRead(header_page_id_);
//   ctx.root_page_id_ = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
//   // TODO(SMLZ) 需要check其他线程是否修改root_page吗？
//   // ANSWER: yes, need to ensure to get the correct root_page
//   // 1. get root_page
//   auto guard = bpm_->FetchPageRead(ctx.root_page_id_);
//   header_guard.Drop();
//   // 2. init current_page and current_guard as root
//   ctx.current_page_id_ = guard.PageId();
//   auto current_page = guard.As<BPlusTreePage>();
//   ctx.current_read_guard_ = std::move(guard);
//   while (!current_page->IsLeafPage()) {
//     auto internal = reinterpret_cast<InternalPage *>(const_cast<BPlusTreePage *>(current_page));
//     // 3. find child entry
//     // TODO(SMLZ) change to binary search
//     int index = 1;
//     for (; index < internal->GetSize(); ++index) {
//       if (comparator_(internal->KeyAt(index), key) == 1) {
//         break;
//       }
//     }
//     --index;
//     // 4. aquire r_lock for next node
//     guard = std::move(bpm_->FetchPageRead(internal->ValueAt(index)));
//     current_page = guard.As<BPlusTreePage>();
//     ctx.current_page_id_ = guard.PageId();
//     if (current_page->IsLeafPage()) {
//       // 5. aquire write lock for leaf
//       // because  have the patent's read lock, no need to worry other thread update leaf of parent
//       guard.Drop();
//       ctx.current_write_guard_ = bpm_->FetchPageWrite(ctx.current_page_id_);
//       current_page = ctx.current_write_guard_.AsMut<BPlusTreePage>();
//       break;
//     }
//     // 5. release r_lock for previous node
//     ctx.current_read_guard_ = std::move(guard);
//   }
//   // 6. upgrade r_lock for leaf node to w_lock
//   ctx.current_read_guard_.Drop();
//   ctx.current_write_guard_ = bpm_->FetchPageWrite(ctx.current_page_id_);
//   return reinterpret_cast<LeafPage *>(const_cast<BPlusTreePage *>(current_page));
// }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageDeletePessimistic(const KeyType &key, Context &ctx) -> LeafPage * {
  // use header_page to get root_page_id
  ctx.header_write_guard_ = bpm_->FetchPageWrite(header_page_id_);
  ctx.root_page_id_ = ctx.header_write_guard_.As<BPlusTreeHeaderPage>()->root_page_id_;
  auto guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  ctx.current_page_id_ = guard.PageId();
  auto current_page = guard.As<BPlusTreePage>();
  ctx.write_set_.emplace_back(std::move(guard));
  while (!current_page->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(const_cast<BPlusTreePage *>(current_page));
    // find child entry
    // TODO(SMLZ) change to binary search
    int index = 1;
    for (; index < internal->GetSize(); ++index) {
      if (comparator_(internal->KeyAt(index), key) == 1) {
        break;
      }
    }
    --index;

    guard = std::move(bpm_->FetchPageWrite(internal->ValueAt(index)));
    current_page = guard.As<BPlusTreePage>();
    ctx.current_page_id_ = guard.PageId();
    if (current_page->IsSafeForDelete()) {
      // release all parent's lock from top to current
      ctx.write_set_.clear();
      ctx.header_write_guard_.Drop();
      ctx.is_root_safe_ = true;
    }
    ctx.write_set_.emplace_back(std::move(guard));
  }
  current_page = ctx.write_set_.back().AsMut<LeafPage>();
  // last guard in write_set is always write_guard for leaf_page
  return reinterpret_cast<LeafPage *>(const_cast<BPlusTreePage *>(current_page));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageInsertPessimistic(const KeyType &key, Context &ctx) -> LeafPage * {
  // std::unique_lock<std::shared_mutex> lock(root_mutex_);
  // use header_page to get root_page_id
  // ctx.root_page_id_ = GetRootPageId();
  ctx.header_write_guard_ = bpm_->FetchPageWrite(header_page_id_);
  ctx.root_page_id_ = ctx.header_write_guard_.As<BPlusTreeHeaderPage>()->root_page_id_;
  // TODO(SMLZ) 需要check其他线程是否修改root_page吗？
  auto guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  ctx.current_page_id_ = guard.PageId();
  auto current_page = guard.As<BPlusTreePage>();
  ctx.write_set_.emplace_back(std::move(guard));
  while (!current_page->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(const_cast<BPlusTreePage *>(current_page));
    // find child entry
    // TODO(SMLZ) change to binary search
    int index = 1;
    for (; index < internal->GetSize(); ++index) {
      if (comparator_(internal->KeyAt(index), key) == 1) {
        break;
      }
    }
    --index;

    guard = std::move(bpm_->FetchPageWrite(internal->ValueAt(index)));
    current_page = guard.As<BPlusTreePage>();
    ctx.current_page_id_ = guard.PageId();
    if (current_page->IsSafeForInsert()) {
      // release all parent's lock from top to current
      ctx.write_set_.clear();
      ctx.header_write_guard_.Drop();
      ctx.is_root_safe_ = true;
    }
    ctx.write_set_.emplace_back(std::move(guard));
  }
  current_page = ctx.write_set_.back().AsMut<LeafPage>();
  // last guard in write_set is always write_guard for leaf_page
  return reinterpret_cast<LeafPage *>(const_cast<BPlusTreePage *>(current_page));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetBrotherNode(InternalPage *child, InternalPage *parent, bool &is_rightmost) -> page_id_t {
  if (comparator_(child->KeyAt(1), parent->KeyAt(parent->GetSize() - 1)) == 1) {
    is_rightmost = true;
    return parent->ValueAt(parent->GetSize() - 2);
  }
  is_rightmost = false;
  int index = 1;
  auto max_child_key = child->KeyAt(child->GetSize() - 1);
  for (; index < parent->GetSize(); ++index) {
    if (comparator_(max_child_key, parent->KeyAt(index)) == 1) {
      break;
    }
  }
  // TODO(SMLZ) handle index out of range
  return parent->ValueAt(index);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetIndexOfChild(KeyType child_key_max, const InternalPage *parent) -> int {
  int index = 1;
  for (; index < parent->GetSize(); ++index) {
    if (comparator_(child_key_max, parent->KeyAt(index)) == -1) {
      break;
    }
  }
  return index - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsRightmostChild(const BPlusTreePage *child, const InternalPage *parent) -> bool {
  // is rightmost when max_key of child no less than max_key of parent
  if (child->IsLeafPage()) {
    auto child_leaf = reinterpret_cast<const LeafPage *>(child);
    auto max_child_key = child_leaf->KeyAt(child_leaf->GetSize() - 1);
    auto max_parent_key = parent->KeyAt(parent->GetSize() - 1);
    return comparator_(max_child_key, max_parent_key) != -1;
  }
  auto child_internal = reinterpret_cast<const InternalPage *>(child);
  return comparator_(child_internal->KeyAt(child_internal->GetSize() - 1), parent->KeyAt(parent->GetSize() - 1)) != -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetBrotherPageID(const BPlusTreePage *child, const InternalPage *parent, bool &is_right_brother)
    -> page_id_t {
  if (child->IsLeafPage()) {
    auto child_leaf = reinterpret_cast<const LeafPage *>(child);
    if (IsRightmostChild(child_leaf, parent)) {
      // left brother
      is_right_brother = false;
      int index = GetIndexOfChild(child_leaf->KeyAt(child_leaf->GetSize() - 1), parent);
      return parent->ValueAt(index - 1);
    }
    is_right_brother = true;
    // right brother
    return child_leaf->GetNextPageId();
  }
  // child is internal page
  auto child_internal = reinterpret_cast<const InternalPage *>(child);
  if (IsRightmostChild(child_internal, parent)) {
    // left brother
    is_right_brother = false;
    int index = GetIndexOfChild(child_internal->KeyAt(child_internal->GetSize() - 1), parent);
    return parent->ValueAt(index - 1);
  }
  // right brother
  is_right_brother = true;
  int index = GetIndexOfChild(child_internal->KeyAt(child_internal->GetSize() - 1), parent);
  return parent->ValueAt(index + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(page_id_t root_page_id, WritePageGuard &header_guard) {
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = root_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetLeafPageInsert(const KeyType &key, Context &ctx) {
  ctx.header_write_guard_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_write_guard_.AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    ctx.is_tree_empty_ = true;
    // return with header write guard
    return;
  }
  ctx.root_page_id_ = ctx.header_write_guard_.As<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.current_write_guard_ = bpm_->FetchPageWrite(ctx.root_page_id_);
  ctx.current_page_id_ = ctx.current_write_guard_.PageId();
  auto current_page = ctx.current_write_guard_.As<BPlusTreePage>();
  ctx.write_set_.emplace_back(std::move(ctx.current_write_guard_));
  while (!current_page->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(const_cast<BPlusTreePage *>(current_page));
    // find child entry
    int index = internal->FindKeyPosition(key, comparator_);
    ctx.index_set_.push_back(index);
    auto temp = internal->ValueAt(index);
    if (internal->GetSize() == 0) {
      // std::cout << this->DrawBPlusTree() << std::endl;
    }
    ctx.current_write_guard_ = std::move(bpm_->FetchPageWrite(temp));
    ctx.current_page_id_ = ctx.current_write_guard_.PageId();
    current_page = ctx.current_write_guard_.As<BPlusTreePage>();
    if (current_page->IsSafeForInsert()) {
      // release all parent's lock from top to current
      ctx.write_set_.clear();
      ctx.header_write_guard_.Drop();
    }
    ctx.write_set_.emplace_back(std::move(ctx.current_write_guard_));
  }
  // last guard in write_set is always write_guard for leaf_page
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetLeafPageDelete(const KeyType &key, Context &ctx) {
  ctx.header_write_guard_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_write_guard_.AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    ctx.is_tree_empty_ = true;
    // return with header write guard
    return;
  }
  ctx.root_page_id_ = ctx.header_write_guard_.As<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.current_write_guard_ = bpm_->FetchPageWrite(ctx.root_page_id_);
  ctx.current_page_id_ = ctx.current_write_guard_.PageId();
  auto current_page = ctx.current_write_guard_.As<BPlusTreePage>();
  ctx.write_set_.emplace_back(std::move(ctx.current_write_guard_));
  while (!current_page->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(const_cast<BPlusTreePage *>(current_page));
    // find child entry
    int index = internal->FindKeyPosition(key, comparator_);
    ctx.index_set_.push_back(index);
    ctx.current_write_guard_ = std::move(bpm_->FetchPageWrite(internal->ValueAt(index)));
    ctx.current_page_id_ = ctx.current_write_guard_.PageId();
    current_page = ctx.current_write_guard_.As<BPlusTreePage>();
    if (current_page->IsSafeForDelete()) {
      // release all parent's lock from top to current
      ctx.write_set_.clear();
      ctx.header_write_guard_.Drop();
    }
    ctx.write_set_.emplace_back(std::move(ctx.current_write_guard_));
  }
  // last guard in write_set is always write_guard for leaf_page
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetLeafPageRead(const KeyType &key, Context &ctx) {
  ctx.current_read_guard_ = bpm_->FetchPageRead(header_page_id_);
  auto header_page = ctx.current_read_guard_.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    ctx.is_tree_empty_ = true;
    // return with header read guard (current_read_guard)
    return;
  }
  ctx.root_page_id_ = header_page->root_page_id_;
  auto guard = bpm_->FetchPageRead(ctx.root_page_id_);
  ctx.previous_read_guard_ = std::move(ctx.current_read_guard_);
  ctx.current_read_guard_ = std::move(guard);
  ctx.current_page_id_ = ctx.current_read_guard_.PageId();
  auto current_page = ctx.current_read_guard_.As<BPlusTreePage>();
  while (!current_page->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(const_cast<BPlusTreePage *>(current_page));
    // find child entry
    int index = internal->FindKeyPosition(key, comparator_);
    ctx.index_set_.push_back(index);
    guard = bpm_->FetchPageRead(internal->ValueAt(index));
    ctx.previous_read_guard_ = std::move(ctx.current_read_guard_);
    ctx.current_read_guard_ = std::move(guard);
    ctx.current_page_id_ = ctx.current_read_guard_.PageId();
    current_page = ctx.current_read_guard_.As<BPlusTreePage>();
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
