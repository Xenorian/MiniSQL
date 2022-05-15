#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(BufferPoolManager *buffer_pool_manager, Schema *schema, LogManager *log_manager,
LockManager *lock_manager, RowId row_record)
  : buffer_pool_manager_(buffer_pool_manager),
    schema_(schema),
    log_manager_(log_manager),
    lock_manager_(lock_manager),
    row_record_(row_record),
    tuple(row_record_) {}

TableIterator::TableIterator(const TableIterator &other)
  : buffer_pool_manager_(other.buffer_pool_manager_),
    schema_(other.schema_),
    log_manager_(other.log_manager_),
    lock_manager_(other.lock_manager_),
    row_record_(other.row_record_),
    tuple(other.tuple) {}


TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const {
  return row_record_ == itr.row_record_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(row_record_ == itr.row_record_);
}

const Row &TableIterator::operator*() {
  // ASSERT(false, "Not implemented yet.");
  return tuple;
}

Row *TableIterator::operator->() {
  return &tuple;
}

TableIterator &TableIterator::operator++() {
  // BufferPoolManager *buffer_pool_manager = buffer_pool_manager_;
  auto cur_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(row_record_.GetPageId()));
  cur_page->RLatch();
  assert(cur_page != nullptr);  // all pages are pinned

  RowId next_tuple_rid; //the RowId for the next tuple
  if (!cur_page->GetNextTupleRid(row_record_, &next_tuple_rid)) {  // cannot get the next tuple: end of this page
    while (cur_page->GetNextPageId() != INVALID_PAGE_ID) { // if the next page is valid
      auto next_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page->GetNextPageId()));
      cur_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
      cur_page = next_page; // turn to the next page
      cur_page->RLatch();
      if (cur_page->GetFirstTupleRid(&next_tuple_rid)) { //get the first tuple in the next page 
        break; // endwhile
      }
    }
  }
  row_record_ = next_tuple_rid; // save the RowId
  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row_record_.GetPageId()))
        ->GetTuple(&tuple, schema_, txn_, lock_manager_); // save the tuple
  // release until copy the tuple
  cur_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator itr(*this);
  ++(*this);
  return TableIterator(itr);
}
