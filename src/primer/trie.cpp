#include "primer/trie.h"
#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <stack>
#include <string_view>
#include <utility>
#include <vector>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  auto root = this->root_;
  if (root == nullptr) {
    return nullptr;
  }
  size_t i = 0;
  std::shared_ptr<const TrieNode> current_node = root;
  while (i < key.length()) {
    auto child_it = current_node->children_.find(key[i]);
    if (child_it != current_node->children_.end()) {
      current_node = child_it->second;
    } else {
      current_node = nullptr;
      break;
    }
    ++i;
  }
  if (current_node == nullptr) {
    return nullptr;
  }
  auto dst_node = dynamic_cast<const TrieNodeWithValue<T> *>(current_node.get());
  if (dst_node == nullptr) {
    return nullptr;
  }
  return dst_node->value_.get();
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  // 任意时刻从任意版本的根节点遍历都可得到该时刻下的子节点
  // 插入时需要从底至上新建插入节点及其所有父节点
  // 构造函数表明只能从下至上构造，结构体只存了子节点表明只能自上而下遍历，需要存储父节点信息

  std::unique_ptr<TrieNode> copy_node;
  auto old_root = this->root_;
  std::stack<std::shared_ptr<const TrieNode>> parents_stack;
  size_t i = 0;
  auto current_node = old_root;
  parents_stack.push(current_node);
  // 自顶向下遍历，获取父节点信息
  while (i < key.length()) {
    if (current_node != nullptr) {
      auto child_it = current_node->children_.find(key[i]);
      if (child_it != current_node->children_.end()) {
        // 匹配当前字符
        current_node = child_it->second;
      } else {
        // 从此处开始不匹配，后续节点均需新建且无兄弟
        current_node = nullptr;
      }
    }
    parents_stack.push(current_node);
    ++i;
  }
  // 自底向上构造
  auto v = std::make_shared<T>(std::move(value));
  auto old_parent_node = parents_stack.top();
  parents_stack.pop();
  std::shared_ptr<const TrieNodeWithValue<T>> final_node;
  if (old_parent_node != nullptr) {
    // 修改或者为中间节点添加值
    final_node = std::make_shared<const TrieNodeWithValue<T>>(old_parent_node->children_, v);
  } else {
    // 插入新节点
    final_node = std::make_shared<const TrieNodeWithValue<T>>(v);
  }
  current_node = final_node;

  while (i >= 1) {
    old_parent_node = parents_stack.top();
    parents_stack.pop();
    if (old_parent_node == nullptr) {
      copy_node = std::make_unique<TrieNode>(std::map{std::make_pair(key[i - 1], current_node)});
    } else {
      copy_node = old_parent_node->Clone();
      copy_node->children_[key[i - 1]] = current_node;
      // 为啥下面的不能覆盖？
      // copy_node->children_.insert(std::make_pair(key[i-1], current_node));
    }
    current_node = std::move(copy_node);
    --i;
  }
  return Trie{current_node};

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  std::unique_ptr<TrieNode> copy_node;
  auto old_root = this->root_;
  std::stack<std::shared_ptr<const TrieNode>> parents_stack;
  size_t i = 0;
  // old_root入栈
  auto current_node = old_root;
  parents_stack.push(current_node);
  // 自顶向下遍历整个key，获取父节点信息存入栈中，（不存在节点以nullptr填充？）
  while (i < key.length()) {
    if (current_node == nullptr) {
      //?
      continue;
    }
    auto child_it = current_node->children_.find(key[i]);
    if (child_it != current_node->children_.end()) {
      // 匹配当前字符
      current_node = child_it->second;
    } else {
      // 从此处开始不匹配(remove 可能会有不匹配嘛？)
      current_node = nullptr;
    }
    parents_stack.push(current_node);
    ++i;
  }

  auto old_parent_node = parents_stack.top();
  parents_stack.pop();
  if (old_parent_node == nullptr) {
    // TODO(unknown):
  }
  if (old_parent_node->children_.empty()) {
    // 为叶节点
    // 递归找到第一个含值或有其他孩子的父节点
    while (true) {
      if (parents_stack.empty()) {
        old_parent_node = nullptr;
        break;
      }
      old_parent_node = parents_stack.top();
      parents_stack.pop();
      if (old_parent_node->children_.size() > 1 || old_parent_node->is_value_node_) {
        break;
      }
      --i;
    }
    if (old_parent_node == nullptr) {
      // 删除后字典树为空
      return Trie{};
    }

    copy_node = old_parent_node->Clone();
    copy_node->children_.erase(key[i - 1]);
    current_node = std::move(copy_node);
    --i;
  } else {
    // 中间含值节点
    // 转化一个不含值的TrieNode
    // current_node修改为当前的TrieNode
    auto node_without_value = std::make_shared<const TrieNode>(old_parent_node->children_);
    current_node = node_without_value;
  }

  // 从新节点生成树
  while (i >= 1) {
    old_parent_node = parents_stack.top();
    parents_stack.pop();
    copy_node = old_parent_node->Clone();
    // 更新子节点
    copy_node->children_[key[i - 1]] = current_node;
    // copy_node->children_.insert(std::make_pair(key[i - 1], current_node));
    current_node = std::move(copy_node);
    --i;
  }
  return Trie{current_node};
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
