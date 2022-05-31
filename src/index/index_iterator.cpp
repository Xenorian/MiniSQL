#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(
    page_id_t first_page_id ,int index, BufferPoolManager *bpm) {
  valid = true;
  index_now = index;
  my_buffer_pool_mangaer = bpm;
  leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
      my_buffer_pool_mangaer->FetchPage(first_page_id)->GetData());
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() { 
  if (valid == true)
    return leaf_page->GetItem(index_now);
  else
    // dangerous
  {
    static MappingType invalid = make_pair(KeyType{}, ValueType{});
    return invalid;
  }
    
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  index_now++;
  if (index_now <= leaf_page->GetSize() - 1) {
    //fine
  } else {
    if (leaf_page->GetNextPageId() == INVALID_PAGE_ID) {
      my_buffer_pool_mangaer->UnpinPage(leaf_page->GetPageId(), true);
      valid = false;
      leaf_page = nullptr;
      index_now = 0;
    } else {
      page_id_t next_page_id = leaf_page->GetNextPageId();
      my_buffer_pool_mangaer->UnpinPage(leaf_page->GetPageId(), true);

      leaf_page=reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*>(
          my_buffer_pool_mangaer->FetchPage(next_page_id)->GetData());
      index_now = 0;
    }
  }

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  if (this->valid == itr.valid) {
    if (this->leaf_page == itr.leaf_page && this->index_now == itr.index_now)
      return true;
    else
      return false;
  } else {
    return false;
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { return !((*this) == itr); }

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
