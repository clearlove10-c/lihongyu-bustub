//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // 使用RAII思想管理锁
  std::lock_guard<std::mutex> lock(latch_);
  if (!inf_lru_list_.empty()) {
    // inf链表不为空
    // 从表尾向前遍历找到可删除节点，使用反向迭代器
    auto rit = inf_lru_list_.rbegin();
    for (; rit != inf_lru_list_.rend(); ++rit) {
      if (rit->is_evictable_) {
        break;
      }
    }
    if (rit != inf_lru_list_.rend()) {
      // inf链表中找到可删除节点
      *frame_id = rit->fid_;
      // 将其从inf列表中删除（直接使用base得到的迭代器在它后一位）
      inf_lru_list_.erase((++rit).base());
      // 将其从node_store_中删除
      node_store_.erase(*frame_id);
      curr_size_--;
      return true;
    }
  }
  // inf链表为空或inf链表中无可删除节点
  // 找到lru_list中可删除节点
  auto rit = lru_list_.rbegin();
  for (; rit != lru_list_.rend(); ++rit) {
    if (rit->is_evictable_) {
      break;
    }
  }
  if (rit != lru_list_.rend()) {
    // 找到可删除节点
    // 从lru_list中删除
    *frame_id = rit->fid_;
    lru_list_.erase((++rit).base());
    // 将其从node_store_中删除
    node_store_.erase(*frame_id);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception::exception();
  }
  auto node_it = node_store_.find(frame_id);
  std::list<LRUKNode>::iterator node;
  if (node_it == node_store_.end()) {
    // 不存在该键
    // 插入到inf表表头
    inf_lru_list_.emplace_front(k_, frame_id);
    node = inf_lru_list_.begin();
    // TODO(SMLZ) 初始化是可驱逐还是不可驱逐？
    node->is_evictable_ = true;
    curr_size_++;
    // 插入到node_store_
    node_store_[frame_id] = node;
  } else {
    node = node_it->second;
  }
  // k_ or node.k_?
  if (node->history_.size() == k_) {
    // history列表已满，此时距离为current_timestamp_-node.history.back()
    // 由于对于当前record操作current_timestamp_相同，此处只关心倒数第k次访问时间，即node.history.back()
    // 也就是说可以把倒数第K次访问时间戳node.history.back()视作-distance（时间戳越小，距离越远）
    // 去除最远访问时间戳，以维持history大小始终为k
    // 什么时候pop？现在就pop
    node->history_.pop_back();
    // 这里有改进：从node所在位置向表头遍历查找，减少了查找次数
    // 原理：node的新位置一定在旧位置的前面
    std::list<LRUKNode>::reverse_iterator rit(node);
    for (; rit != lru_list_.rend(); ++rit) {
      if (rit->history_.back() > node->history_.back()) {
        break;
      }
    }
    // rit为需插入的节点位置的前一个节点
    // STL的移动方法
    // 把node移动到rit的后面
    lru_list_.splice(std::next((++rit).base()), lru_list_, node);
  } else if (node->history_.size() == k_ - 1) {
    // history列表变满
    auto rit = lru_list_.rbegin();
    for (; rit != lru_list_.rend(); ++rit) {
      if (rit->history_.back() > node->history_.back()) {
        break;
      }
    }
    // 把inf_lru_list的node移动到lru_list的it后面
    lru_list_.splice(std::next((++rit).base()), inf_lru_list_, node);
  }
  // 添加当前访问时间戳
  node->history_.push_front(current_timestamp_);
  // 更新时间戳
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  auto node_it = node_store_.find(frame_id);
  if (node_it == node_store_.end()) {
    throw Exception::exception();
  }
  auto node = node_it->second;
  if (node->is_evictable_ ^ set_evictable) {
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }
  node->is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto node_it = node_store_.find(frame_id);
  if (node_it == node_store_.end()) {
    return;
  }
  auto node = node_it->second;
  if (!node->is_evictable_) {
    throw Exception::exception();
  }
  node_store_.erase(node_it);
  if (node->history_.size() == 8) {
    // 在lru_list中
    lru_list_.erase(node);
  } else {
    // 在inf中
    inf_lru_list_.erase(node);
  }
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> s_lock(latch_);
  return curr_size_;
}

}  // namespace bustub
