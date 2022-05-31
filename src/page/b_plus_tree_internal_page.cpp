#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  ASSERT(max_size > 0, "wrong max size");
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetMaxSize(); i++) {
    if (array_[i].second == value) return i;
  }
  //can't find the value, return the 0. Because the array_[0].second == INVALID_PAGE_ID
  return 0;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  ValueType val{array_[index].second};
  return val;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // replace with your own code
  ValueType val{};
  // O(N)
  int place = my_lower_bound(key, comparator);
  val = ValueAt(place);
  return val;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::my_lower_bound(const KeyType &key,const KeyComparator &comparator) const {
  if (GetSize() == 0) return 0;

  int place = 0;
  int i = 0;
  if (comparator(key, array_[1].first) < 0)
    place = 0;
  else {
    for (i = 1; i <= GetSize() - 2; i++) {
      if (comparator(key, array_[i + 1].first) >= 0) {
        continue;
      } else {
        break;
      }
    }
  }
  place = i;
  return place;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const KeyType &old_key ,const ValueType &old_value,
                                                     const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].first = old_key;
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  IncreaseSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int point = ValueIndex(old_value);
  // move the later pair
  for (int i = GetSize(); i >= point + 2; i--) {
    array_[i] = array_[i - 1];
  }
  // insert
  array_[point + 1].first = new_key;
  array_[point + 1].second = new_value;
  IncreaseSize(1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::EditNode(const ValueType &old_value, const KeyType &new_key
                                                    ) {
  int point = ValueIndex(old_value);
  array_[point].first = new_key;
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  /*
  * single version
  int start = GetMinSize();
  int end = GetSize() - 1;
  //move
  for (int i = start; i <= end; i++) {
    recipient->array_[i - start] = array_[i];
  }
  //set the size
  recipient->SetSize(end - start + 1);
  SetSize(start);
  */
  int size_moved = GetSize() - GetMinSize();
  recipient->CopyNFrom(array_ + GetMinSize(), size_moved, buffer_pool_manager);

  //set the size of this page
  IncreaseSize(-size_moved);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  //for test
  ASSERT(GetSize() + size < GetMaxSize(), "out of range");

  int ptr_now = GetSize();
  BPlusTreePage *ptr_son = NULL;
  for (int i = 0; i < size; i++) {
    array_[ptr_now + i] = items[i];
    // adapt the sons' parent page id
    ptr_son = reinterpret_cast<BPlusTreePage*> (buffer_pool_manager->FetchPage(array_[ptr_now+i].second));
    ptr_son->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(array_[ptr_now+i].second,true);
  }
  
  //set size of this page
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int start = index;
  int end = GetSize();
  
  for (int i = start; i <=end-1; i++) {
    array_[i] = array_[i + 1];
  }
  //set the size
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
  ValueType val{};
  //get
  val = array_[0].second;
  //remove
  IncreaseSize(-1);

  return val;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */

// called by the right
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient,
                                               BufferPoolManager *buffer_pool_manager) {
    
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  //set the size
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */

//from right to left, called by the right
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient,
                                                      BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(array_[0], buffer_pool_manager);
  int start = 0;
  int end = GetSize() - 2;
  for (int i = start; i <= end; i++) array_[i] = array_[i + 1];
  // set the size
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  //copy
  array_[GetSize()] = pair;
  // set the size
  IncreaseSize(1);
  // change the son's parent id
  BPlusTreeInternalPage *son = reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager->FetchPage(pair.second));
  son->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(array_[GetSize() - 1], buffer_pool_manager);
  //set size
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // move
  int start = 1;
  int end = GetSize();
  for (int i = end; i >= start; i--) array_[i] = array_[i - 1];
  // copy
  array_[0] = pair;
  // set the size
  IncreaseSize(1);
  // change the son's parent id
  BPlusTreeInternalPage *son = reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager->FetchPage(pair.second));
  son->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;