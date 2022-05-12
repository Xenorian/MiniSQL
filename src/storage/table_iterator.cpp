#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(RowId record_id_first, BufferPoolManager *buffer_pool_manager, Schema *schema,
                             LogManager *log_manager, LockManager *lock_manager)
    : buffer_pool_manager_(buffer_pool_manager),
      schema_(schema),
      log_manager_(log_manager),
      lock_manager_(lock_manager),
      record_id_now_(record_id_first),
      record_now_(record_id_first) {}

TableIterator::TableIterator(const TableIterator &other)
    : buffer_pool_manager_(other.buffer_pool_manager_),
      schema_(other.schema_),
      log_manager_(other.log_manager_),
      lock_manager_(other.lock_manager_),
      record_id_now_(other.record_id_now_),
      record_now_(other.record_now_) {
  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_id_now_.GetPageId()))
      ->GetTuple(&record_now_, schema_, txn, lock_manager_);
}

TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const { return (this->record_id_now_ == itr.record_id_now_); }

bool TableIterator::operator!=(const TableIterator &itr) const { return !(this->record_id_now_ == itr.record_id_now_); }

const Row &TableIterator::operator*() {
  // ASSERT(false, "Not implemented yet.");
  return record_now_;
}

Row *TableIterator::operator->() { return &record_now_; }

TableIterator &TableIterator::operator++() {
  // point to the next slot
  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_id_now_.GetPageId()))
      ->GetNextTupleRid(record_id_now_, &record_id_now_);
  // try to fetch the tuple
  if(reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_id_now_.GetPageId()))
          ->GetTuple(&record_now_, schema_, txn, lock_manager_) == false) {
    //try to get the first tuple in next page
    record_id_now_.Set(
        reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_id_now_.GetPageId()))->GetNextPageId(), 0);

    reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_id_now_.GetPageId()))
        ->GetFirstTupleRid(&record_id_now_);
    //get the tuple
    reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_id_now_.GetPageId()))
        ->GetTuple(&record_now_, schema_, txn, lock_manager_);
  }
  return *this;
}

TableIterator TableIterator::operator++(int) { // point to the next slot
  TableIterator tmp(*this);
  // point to the next slot
  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_id_now_.GetPageId()))
      ->GetNextTupleRid(record_id_now_, &record_id_now_);
  // try to fetch the tuple
  if (reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_id_now_.GetPageId()))
          ->GetTuple(&record_now_, schema_, txn, lock_manager_) == false) {
    // try to get the first tuple in next page
    record_id_now_.Set(
        reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_id_now_.GetPageId()))->GetNextPageId(), 0);

    reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_id_now_.GetPageId()))
        ->GetFirstTupleRid(&record_id_now_);
    // get the tuple
    reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_id_now_.GetPageId()))
        ->GetTuple(&record_now_, schema_, txn, lock_manager_);
  }
  return TableIterator(tmp);
}
