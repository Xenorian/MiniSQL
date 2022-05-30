#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/*
root page edit:
ctor
StartNewTree
InsertIntoLeaf -> Split and no parent
InsertIntoParent -> Split and parent is root
Delete -> colapse and parent is root and root has only one child -> Adjust root
Delete -> fully delete -> 
*/
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
  root_page_id_ = INVALID_PAGE_ID;
  UpdateRootPageId(true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy_subtree(page_id_t root) {
  if (root == INVALID_PAGE_ID) return;

  BPlusTreePage *root_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root)->GetData());
  if (root_page->IsLeafPage() == true) {
    buffer_pool_manager_->UnpinPage(root, false);
    buffer_pool_manager_->DeletePage(root);
  } else {
    InternalPage *this_page = reinterpret_cast<InternalPage *> (root_page);
    int start=0;
    int end = this_page->GetSize()-1;
    for (int i = start; i <= end; i++) {
      Destroy_subtree(this_page->ValueAt(i));
    }
    buffer_pool_manager_->UnpinPage(root, false);
    buffer_pool_manager_->DeletePage(root);
  }

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  if (root_page_id_ == INVALID_PAGE_ID) return;

  BPlusTreePage *root = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  Destroy_subtree(root->GetPageId());
  root_page_id_ = INVALID_PAGE_ID;

  IndexRootsPage *header_page =
      reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  header_page->Delete(index_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { 
  if (root_page_id_ == INVALID_PAGE_ID)
    return true;
  else
    return false;
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
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  //empty
  if (IsEmpty() == true) return false;

  BPlusTreePage *now = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  InternalPage *inter_page = nullptr;
  LeafPage *leaf_page = nullptr;
  bool suc = false;
  ValueType tmp;

  while (now->IsLeafPage() == false) {
    inter_page = reinterpret_cast<InternalPage *>(now);
    now = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(inter_page->Lookup(key, comparator_))->GetData());
    buffer_pool_manager_->UnpinPage(inter_page->GetPageId(), false);
  }

  if (now->IsLeafPage() == true) {
    leaf_page = reinterpret_cast<LeafPage *>(now);
    suc = leaf_page->Lookup(key, tmp, comparator_);
    if (suc == true) {
      result.push_back(tmp);
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  }

  return suc;
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  //BPlusTreePage *now = nullptr;
  bool suc = false;
  //empty
  if (IsEmpty() == true) {
    StartNewTree(key, value);
    if (root_page_id_ == INVALID_PAGE_ID)
      suc = false;
    else
      suc = true;

  } else {
    //not empty
    suc = InsertIntoLeaf(key, value, transaction);
  }

  return suc;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  LeafPage *now = nullptr;
  now = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(root_page_id_)->GetData());
  UpdateRootPageId(false);
  if (root_page_id_ == INVALID_PAGE_ID) throw "out of memory";
  //allocate success
  now->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  now->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(now->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  BPlusTreePage *now = nullptr;
  bool suc = false;
  now = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

  InternalPage *inter_page = nullptr;
  LeafPage *leaf_page = nullptr;
  LeafPage *new_page = nullptr;
  ValueType tmp;

  while (now->IsLeafPage() == false) {
    inter_page = reinterpret_cast<InternalPage *>(now);
    int place = inter_page->my_lower_bound( key, comparator_);
    now = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(inter_page->ValueAt(place))->GetData());
    buffer_pool_manager_->UnpinPage(inter_page->GetPageId(), false);
  }

  if (now->IsLeafPage() == true) {
    leaf_page = reinterpret_cast<LeafPage *>(now);
    suc = !(leaf_page->Lookup(key, tmp, comparator_));
    // not found
    if (suc == true) {
      //1. insert first
      leaf_page->Insert(key, value, comparator_);
      // edit the parent

      InternalPage *parent = nullptr;
      page_id_t inter_page_id = INVALID_PAGE_ID;
      page_id_t old_page_id = INVALID_PAGE_ID;
      int index = 0;
      KeyType new_key{};
      inter_page_id = leaf_page->GetParentPageId();
      index = leaf_page->KeyIndex(key,comparator_);
      new_key = leaf_page->KeyAt(0);
      old_page_id = leaf_page->GetPageId();
      while (index == 0 && inter_page_id != INVALID_PAGE_ID) {
        // 1.set key
        parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(inter_page_id)->GetData());
        index = parent->ValueIndex(old_page_id);
        parent->SetKeyAt(index, new_key);
        new_key = parent->KeyAt(0);
        old_page_id = inter_page_id;
        inter_page_id = parent->GetParentPageId();
        buffer_pool_manager_->UnpinPage(old_page_id, true);
      }

      //2. check the split
      if (leaf_page->GetSize() <= leaf_page->GetMaxSize()) {
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      } else {
        new_page = Split(leaf_page);
        //no parent
        if (leaf_page->GetParentPageId() == INVALID_PAGE_ID) {
          inter_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(root_page_id_)->GetData());
          UpdateRootPageId(false);
          inter_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
          inter_page->PopulateNewRoot(leaf_page->KeyAt(0), leaf_page->GetPageId(), new_page->KeyAt(0), new_page->GetPageId());
          leaf_page->SetParentPageId(root_page_id_);
          new_page->SetParentPageId(root_page_id_);

          buffer_pool_manager_->UnpinPage(root_page_id_, true);
          buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
        }
        //have parent
        else {
          InsertIntoParent(leaf_page, new_page->KeyAt(0), new_page, transaction);
          //Insert(key, value, transaction);
          buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
        }
      }
      
    }
    // found
    else {
      buffer_pool_manager_->UnpinPage(now->GetPageId(), false);
    }
  }

  return suc;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  BPlusTreePage *old_page = reinterpret_cast<BPlusTreePage *>(node);
  BPlusTreePage *new_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->NewPage(new_page_id)->GetData());
  InternalPage *old_inter_page = nullptr;
  InternalPage *new_inter_page = nullptr;
  LeafPage *old_leaf_page = nullptr;
  LeafPage *new_leaf_page = nullptr;

  if (new_page_id == INVALID_PAGE_ID) throw "out of memory";

  if (old_page->IsLeafPage() == true) {
    old_leaf_page =  reinterpret_cast<LeafPage *>(node);
    new_leaf_page = reinterpret_cast<LeafPage *>(new_page);
    new_leaf_page->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
    new_leaf_page->SetNextPageId(old_leaf_page->GetNextPageId());
    old_leaf_page->SetNextPageId(new_page_id);
    old_leaf_page->MoveHalfTo(new_leaf_page);
  } else {
    old_inter_page = reinterpret_cast<InternalPage *>(node);
    new_inter_page = reinterpret_cast<InternalPage *>(new_page);
    new_inter_page->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
    old_inter_page->MoveHalfTo(new_inter_page, buffer_pool_manager_);
  }

  return reinterpret_cast<N *>(new_page);
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  InternalPage *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));
  InternalPage *new_page = nullptr;
  InternalPage *new_root_page = nullptr;
  //InternalPage *inter_page = nullptr;
  //LeafPage *leaf_page = nullptr;

  //1. insert first
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  //2. check split
  if (parent->GetSize() + 1 <= parent->GetMaxSize()) {
    // ok to insert
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  } else {
    new_page = Split(parent);
    if (parent->IsRootPage() == true) {
      // root
      new_root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(root_page_id_)->GetData());
      UpdateRootPageId(false);
      new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      
      parent->SetParentPageId(root_page_id_);
      new_page->SetParentPageId(root_page_id_);
      
      new_root_page->PopulateNewRoot(parent->KeyAt(0), parent->GetPageId(), new_page->KeyAt(0), new_page->GetPageId());
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    } else {
      // not root
      InsertIntoParent(parent, (reinterpret_cast<InternalPage *>(new_page))->KeyAt(0), new_page, transaction);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    }
  }
  
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LeafPage *leaf_page = nullptr;

  InternalPage *inter_page = nullptr;

  page_id_t inter_page_id = INVALID_PAGE_ID;
  page_id_t old_page_id = INVALID_PAGE_ID;
  int index = 0;
  KeyType new_key{};

  leaf_page = FindLeafPage(key);
  if (leaf_page == nullptr) 
      return;
  //1. remove first

  if (leaf_page->KeyIndex(key, comparator_) == 0){
    leaf_page->RemoveAndDeleteRecord(key, comparator_);
    //edit the parent
    inter_page_id = leaf_page->GetParentPageId();
    index = 0;
    new_key = leaf_page->KeyAt(0);
    old_page_id = leaf_page->GetPageId();
    while (index == 0 && inter_page_id != INVALID_PAGE_ID) {
      //1.set key
      inter_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(inter_page_id)->GetData());
      index = inter_page->ValueIndex(old_page_id);
      inter_page->SetKeyAt(index, new_key);
      new_key = inter_page->KeyAt(0);
      old_page_id = inter_page_id; 
      inter_page_id = inter_page->GetParentPageId();
      buffer_pool_manager_->UnpinPage(old_page_id, true);
    }
  } else {
    leaf_page->RemoveAndDeleteRecord(key, comparator_);
  }
  //2. check the situation
  if (leaf_page->GetParentPageId() == INVALID_PAGE_ID) {
    //this is the root, the only node
    if (leaf_page->GetSize() == 0) {
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      buffer_pool_manager_->DeletePage(root_page_id_);
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
    } else {
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }
  } else {
    if (leaf_page->GetSize()<leaf_page->GetMinSize()) {
      CoalesceOrRedistribute(leaf_page, transaction);
    } else {
      //no need to co or redistri
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    }
  }

}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  BPlusTreePage *now = reinterpret_cast<BPlusTreePage *> (node);
  InternalPage *inter_page = nullptr;
  BPlusTreePage *neibor_page = nullptr;
  int max_index = 0;
  int this_index = 0;

  // this isn't root
  // 1. get the index num
  inter_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(now->GetParentPageId())->GetData());
  max_index = inter_page->GetSize() - 1;
  this_index = inter_page->ValueIndex(now->GetPageId());

  if (this_index == max_index)
    neibor_page = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(inter_page->ValueAt(this_index - 1))->GetData());
  else
    neibor_page= reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(inter_page->ValueAt(this_index + 1))->GetData());
  if (neibor_page->GetSize() - 1 >= neibor_page->GetMinSize()) {
    Redistribute<BPlusTreePage>(neibor_page, now, inter_page, max_index, this_index);
  } else {
    Coalesce<BPlusTreePage>(neibor_page, now, inter_page, max_index, this_index, transaction);
  }

  return true;
}

/*
 Move all the key & value pairs from one page to its sibling page, 

 and notify buffer pool manager to delete this page. 
 
 Parent page must be adjusted to take info of deletion into account. 

 Remember to deal with coalesce or redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */ 
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int max_index, int index,
                              Transaction *transaction) {
  //BPlusTreePage *neighbor_page = reinterpret_cast<BPlusTreePage *> (neighbor_node);
  BPlusTreePage *this_page = reinterpret_cast<BPlusTreePage *> (node);
  InternalPage *nei_inter_page = nullptr;
  InternalPage *now_inter_page = nullptr;
  LeafPage *nei_leaf_page = nullptr;
  LeafPage *now_leaf_page = nullptr;
  

  if (this_page->IsLeafPage() == true) {
    now_leaf_page = reinterpret_cast<LeafPage *> (node);
    nei_leaf_page = reinterpret_cast<LeafPage *> (neighbor_node);
    if (index != max_index) {
      //1. transfer
      nei_leaf_page->MoveAllTo(now_leaf_page);
      //2. delete
      parent->Remove(index + 1);
      buffer_pool_manager_->UnpinPage(now_leaf_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(nei_leaf_page->GetPageId(),true);
      buffer_pool_manager_->DeletePage(nei_leaf_page->GetPageId());
    } else {
      // 1. transfer
      now_leaf_page->MoveAllTo(nei_leaf_page);
      // 2. delete
      parent->Remove(index);
      buffer_pool_manager_->UnpinPage(now_leaf_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(nei_leaf_page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(now_leaf_page->GetPageId());
    }
    // 3. upload
    if (parent->GetSize() < parent->GetMinSize()) {
      // 1. is root
      if (parent->IsRootPage() == true) AdjustRoot(parent);
      // 2. not root
      else
        CoalesceOrRedistribute(parent, transaction);
    } else {
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
  } else {
    now_inter_page = reinterpret_cast<InternalPage *> (node);
    nei_inter_page = reinterpret_cast<InternalPage *> (neighbor_node);
    if (index != max_index) {
      // 1. transfer
      nei_inter_page->MoveAllTo(now_inter_page,buffer_pool_manager_);
      // 2. delete
      parent->Remove(index + 1);
      buffer_pool_manager_->UnpinPage(nei_inter_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(now_inter_page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(nei_inter_page->GetPageId());
    }
    else {
      // 1. transfer
      now_inter_page->MoveAllTo(nei_inter_page,buffer_pool_manager_);
      // 2. delete
      parent->Remove(index);
      buffer_pool_manager_->UnpinPage(now_inter_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(nei_inter_page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(now_inter_page->GetPageId());
    }

    // 3. upload
    if (parent->GetSize() < parent->GetMinSize()) {
      // 1. is root
      if (parent->IsRootPage() == true) {
        AdjustRoot(parent);
      }
      // 2. not root
      else {
        CoalesceOrRedistribute(parent, transaction);
      }
    } else {
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
  }

  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node,
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int max_index,
                                  int index) {
  BPlusTreePage *neighbor_page = reinterpret_cast<BPlusTreePage *> (neighbor_node);
  BPlusTreePage *this_page = reinterpret_cast<BPlusTreePage *> (node);
  InternalPage *nei_inter_page = nullptr;
  InternalPage *now_inter_page = nullptr;
  LeafPage *nei_leaf_page = nullptr;
  LeafPage *now_leaf_page = nullptr;

  if (this_page->IsLeafPage() == true) {
    //leaf
    now_leaf_page = reinterpret_cast<LeafPage *> (node);
    nei_leaf_page = reinterpret_cast<LeafPage *> (neighbor_node);
    if (max_index != index) {
      nei_leaf_page->MoveFirstToEndOf(now_leaf_page);
      parent->SetKeyAt(index + 1, nei_leaf_page->KeyAt(0));
    } else {
      nei_leaf_page->MoveLastToFrontOf(now_leaf_page);
      parent->SetKeyAt(index, now_leaf_page->KeyAt(0));
    }
  } else {
    // lnternal
    now_inter_page = reinterpret_cast<InternalPage *> (node);
    nei_inter_page = reinterpret_cast<InternalPage *> (neighbor_node);
    if (max_index != index) {
      nei_inter_page->MoveFirstToEndOf(now_inter_page,buffer_pool_manager_);
      parent->SetKeyAt(index + 1, nei_inter_page->KeyAt(0));
    } else {
      nei_inter_page->MoveLastToFrontOf(now_inter_page, buffer_pool_manager_);
      parent->SetKeyAt(index, now_inter_page->KeyAt(0));
    }
  }

  buffer_pool_manager_->UnpinPage(this_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { 
  if (old_root_node->GetSize() >= 2) return false;
  
  InternalPage *old_root_page = reinterpret_cast<InternalPage *>(old_root_node);
  BPlusTreePage *new_root_page =
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(old_root_page->ValueAt(0)));

  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  buffer_pool_manager_->DeletePage(root_page_id_);
  //set
  root_page_id_ = new_root_page->GetPageId();
  UpdateRootPageId(false);
  new_root_page->SetParentPageId(INVALID_PAGE_ID);
  buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);

  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  BPlusTreePage *now = nullptr;
  InternalPage *inter_page = nullptr;
  now = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  page_id_t leaf_id = INVALID_PAGE_ID;

  while (now->IsLeafPage() == false) {
    inter_page = reinterpret_cast<InternalPage *>(now);
    int place = 0;
    now = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(inter_page->ValueAt(place))->GetData());
    buffer_pool_manager_->UnpinPage(inter_page->GetPageId(), false);
  }

  leaf_id = now->GetPageId();
  buffer_pool_manager_->UnpinPage(leaf_id, false);
  return INDEXITERATOR_TYPE(leaf_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  LeafPage *now = FindLeafPage(key);
  int index_now = now->KeyIndex(key,comparator_);
  page_id_t leaf_id = now->GetPageId();
  buffer_pool_manager_->UnpinPage(leaf_id, false);
  return INDEXITERATOR_TYPE(leaf_id,index_now ,buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() { return INDEXITERATOR_TYPE(false); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) {
  BPlusTreePage *now = nullptr;
  now = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

  InternalPage *inter_page = nullptr;

  while (now->IsLeafPage() == false) {
    inter_page = reinterpret_cast<InternalPage *>(now);
    int place = inter_page->my_lower_bound(key, comparator_);
    now = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(inter_page->ValueAt(place))->GetData());
    buffer_pool_manager_->UnpinPage(inter_page->GetPageId(), false);
  }

  if (now->IsLeafPage() == true)
    return reinterpret_cast<LeafPage *>(now);
  else
    return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  IndexRootsPage *header_page =
      reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if (insert_record)
    header_page->Insert(index_id_, root_page_id_);
  else
    header_page->Update(index_id_, root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::EditRoot(page_id_t new_page_id) { 
  if (root_page_id_ == INVALID_PAGE_ID) {
    root_page_id_ = new_page_id;
    UpdateRootPageId(true);
  } else {
    root_page_id_ = new_page_id;
    UpdateRootPageId(false);
  }

}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
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
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i >= 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
