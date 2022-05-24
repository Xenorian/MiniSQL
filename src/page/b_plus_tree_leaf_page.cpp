#include <algorithm>
#include <vector>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) { 
  ASSERT(max_size > 0, "wrong max size");
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id); 
  SetNextPageId(INVALID_PAGE_ID);
  SetSize(0);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const { 
  int start = 0;
  int end = GetSize();
  int mid = (start + end) / 2;
  while (start <= end) {
    if (comparator(key, array_[mid].first) < 0) {
      end = mid - 1;
      mid = (start + end) / 2;
    } else if (comparator(key, array_[mid].first) == 0) {
      break;
    } else {
      start = mid + 1;
      mid = (start + end) / 2;
    }
  }

  ASSERT(start <= end, "NOT FOUND");
  return mid;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key{};
  key = array_[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::my_lower_bound(const KeyType& key, const KeyComparator& comp) const {
  if (GetSize() == 0) return 0;
    
  int start = 0, end = GetSize() - 1, mid = 0;
  while (start < end) {
    mid = start + (end-start) / 2;
    if (comp(array_[mid].first,key)>=0) {
      end = mid;
    } else
      start = mid + 1;
  }
  if (comp(array_[start].first,key)<0) start++;
  return start;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  if (GetSize() == 0) {
  } else {
    ASSERT(comparator(array_[my_lower_bound(key, comparator)].first, key) != 0, "Already exists");
  }

  int place = my_lower_bound(key, comparator);
  int start = place + 1;
  int end = GetSize();
  for (int i = end; i >= start; i--) array_[i] = array_[i - 1];
  array_[place].first = key;
  array_[place].second = value;
  //set the size
  IncreaseSize(1);

  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int size_moved = GetSize() - GetMinSize();
  recipient->CopyNFrom(array_ + GetMinSize(), size_moved);

  // set the size of this page
  IncreaseSize(-size_moved);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  // for test
  ASSERT(GetSize() + size < GetMaxSize(), "out of range");

  int ptr_now = GetSize();
  for (int i = 0; i < size; i++) {
    array_[ptr_now + i] = items[i];
  }

  // set size of this page
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  int index = my_lower_bound( key, comparator);
  bool flag = false;
  if (comparator(array_[index].first,key)==0) {
    flag = true;
    value = array_[index].second;
  } else {
    flag = false;
  }

  return flag;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  ValueType not_used;
  bool exits = Lookup(key, not_used, comparator);
  if (exits == true) {
    //move and cover
    int start = KeyIndex(key, comparator);
    int end = GetSize() - 2;
    for (int i = start; i <= end; i++) {
      array_[i] = array_[i + 1];
    }
    //set the size
    IncreaseSize(-1);
  }

  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) { 
    recipient->CopyNFrom(array_, GetSize()); 
    //set size
    SetSize(0);
    //edit the next_page_id
    recipient->SetNextPageId(GetNextPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient){ 
    recipient->CopyLastFrom(array_[0]);
    //move
    int start = 0;
    int end = GetSize() - 2;
    for (int i = start; i <= end; i++) array_[i] = array_[i + 1];
    //edit size
    IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  // copy
  array_[GetSize()] = item;
  // set the size
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyFirstFrom(array_[GetSize() - 1]);
  // set size
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  // move
  int start = 1;
  int end = GetSize();
  for (int i = end; i >= start; i--) array_[i] = array_[i - 1];
  // copy
  array_[0] = item;
  // set the size
  IncreaseSize(1);
}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;