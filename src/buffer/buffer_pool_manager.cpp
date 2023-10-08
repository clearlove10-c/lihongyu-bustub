//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!GetFreeFrame(&frame_id)) {
    return nullptr;
  }
  auto page = &pages_[static_cast<int>(frame_id)];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
  page_table_.erase(page->GetPageId());
  // std::unique_lock<std::mutex> lock(latch_);
  *page_id = AllocatePage();
  // lock.unlock();
  page->page_id_ = *page_id;
  page->ResetMemory();
  page->pin_count_ = 1;
  // 更新相关表
  page_table_[*page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  auto page_table_it = page_table_.find(page_id);
  if (page_table_it != page_table_.end()) {
    auto page = &pages_[static_cast<int>(page_table_it->second)];
    replacer_->SetEvictable(page_table_it->second, false);
    page->pin_count_++;
    return page;
  }

  if (!GetFreeFrame(&frame_id)) {
    return nullptr;
  }

  auto page = &pages_[static_cast<int>(frame_id)];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
  page_table_.erase(page->GetPageId());
  page->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, page->data_);
  page->pin_count_++;
  // 更新相关表
  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto page_table_it = page_table_.find(page_id);
  if (page_table_it == page_table_.end()) {
    // 没找到page_id
    return false;
  }
  auto page = &pages_[static_cast<int>(page_table_it->second)];
  page->is_dirty_ = page->IsDirty() ? true : is_dirty;
  if (page->pin_count_ == 0) {
    return false;
  }
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(page_table_it->second, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto page_table_it = page_table_.find(page_id);
  if (page_table_it == page_table_.end()) {
    // 没找到page_id
    return false;
  }
  auto page = &pages_[static_cast<int>(page_table_it->second)];
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (auto p : page_table_) {
    auto page = &pages_[static_cast<int>(p.second)];
    disk_manager_->WritePage(p.first, page->GetData());
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto page_table_it = page_table_.find(page_id);
  if (page_table_it == page_table_.end()) {
    // 没找到page_id
    return true;
  }
  auto page = &pages_[static_cast<int>(page_table_it->second)];
  if (page->GetPinCount() != 0) {
    return false;
  }

  replacer_->Remove(page_table_it->second);
  free_list_.push_front(page_table_it->second);
  page_table_.erase(page_table_it);
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->ResetMemory();
  // std::lock_guard<std::mutex> lock(latch_);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = this->FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = this->FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = this->FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = this->NewPage(page_id);
  return {this, page};
}

auto BufferPoolManager::GetFreeFrame(frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Evict(frame_id)) {
      return false;
    }
  }
  return true;
}

}  // namespace bustub
