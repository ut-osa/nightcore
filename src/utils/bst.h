#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

template<class T>
class RankingBST {
public:
    static constexpr size_t kDefaultInitialNodePoolSize = 100000;

    explicit RankingBST(size_t initial_node_pool_size = kDefaultInitialNodePoolSize);
    ~RankingBST();

    void Clear();
    size_t Size();
    void Insert(T value);
    bool GetKthElement(size_t kth, T* value);

private:
    struct Node {
        Node* left_child;
        Node* right_child;
        T value;
        size_t size;
    };

    Node* root_;
    std::vector<Node*> node_pool_;

    Node* NewNode(T value);
    void InsertInternal(Node** current_node, Node* new_node);
    void Maintain(Node* node);
    T GetKthElementInternal(Node* current_node, size_t kth);

    DISALLOW_COPY_AND_ASSIGN(RankingBST);
};

template<class T>
RankingBST<T>::RankingBST(size_t initial_node_pool_size) {
    root_ = nullptr;
    node_pool_.resize(initial_node_pool_size);
    for (size_t i = 0; i < initial_node_pool_size; i++) {
        Node* node = new Node;
        node->left_child = nullptr;
        node->right_child = nullptr;
        node_pool_[i] = node;
    }
}

template<class T>
RankingBST<T>::~RankingBST() {
    while (!node_pool_.empty()) {
        Node* node = node_pool_.back();
        node_pool_.pop_back();
        if (node->left_child != nullptr) {
            node_pool_.push_back(node->left_child);
        }
        if (node->right_child != nullptr) {
            node_pool_.push_back(node->right_child);
        }
        delete node;
    }
}

template<class T>
void RankingBST<T>::Clear() {
    node_pool_.push_back(root_);
    root_ = nullptr;
}

template<class T>
size_t RankingBST<T>::Size() {
    return (root_ == nullptr) ? 0 : root_->size;
}

template<class T>
void RankingBST<T>::Insert(T value) {
    InsertInternal(&root_, NewNode(value));
}

template<class T>
bool RankingBST<T>::GetKthElement(size_t kth, T* value) {
    if (root_ == nullptr || kth >= root_->size) {
        return false;
    }
    *value = GetKthElementInternal(root_, kth);
    return true;
}

template<class T>
typename RankingBST<T>::Node* RankingBST<T>::NewNode(T value) {
    Node* node;
    if (node_pool_.empty()) {
        node = new Node;
        node->left_child = nullptr;
        node->right_child = nullptr;
    } else {
        node = node_pool_.back();
        node_pool_.pop_back();
        if (node->left_child != nullptr) {
            node_pool_.push_back(node->left_child);
            node->left_child = nullptr;
        }
        if (node->right_child != nullptr) {
            node_pool_.push_back(node->right_child);
            node->right_child = nullptr;
        }
    }
    node->value = value;
    node->size = 1;
    return node;
}

template<class T>
void RankingBST<T>::InsertInternal(Node** current_node, Node* new_node) {
    if (*current_node == nullptr) {
        *current_node = new_node;
        return;
    }
    T current_value = (*current_node)->value;
    if (new_node->value < current_value) {
        InsertInternal(&(*current_node)->left_child, new_node);
    } else {
        InsertInternal(&(*current_node)->right_child, new_node);
    }
    Maintain(*current_node);
}

template<class T>
void RankingBST<T>::Maintain(Node* node) {
    node->size = 1;
    if (node->left_child != nullptr) {
        node->size += node->left_child->size;
    }
    if (node->right_child != nullptr) {
        node->size += node->right_child->size;
    }
}

template<class T>
T RankingBST<T>::GetKthElementInternal(Node* current_node, size_t kth) {
    DCHECK(current_node != nullptr);
    size_t left_size = current_node->left_child == nullptr ? 0 : current_node->left_child->size;
    if (kth < left_size) {
        return GetKthElementInternal(current_node->left_child, kth);
    } else if (kth == left_size) {
        return current_node->value;
    } else {
        return GetKthElementInternal(current_node->right_child, kth - (left_size + 1));
    }
}

}  // namespace utils
}  // namespace faas
