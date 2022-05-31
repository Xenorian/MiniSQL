#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(RowId record_id_first, BufferPoolManager *buffer_pool_manager, Schema *schema,
                             LogManager *log_manager, LockManager *lock_manager)
    : buffer_pool_manager_(buffer_pool_manager),
      schema_(schema),
      log_manager_(log_manager),
      lock_manager_(lock_manager),
      record_now_(record_id_first) {
  if (!(record_id_first==INVALID_ROWID))
  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_now_.GetRowId().GetPageId()))
      ->GetTuple(&record_now_, schema_, txn, lock_manager_);
}

TableIterator::TableIterator(const TableIterator &other)
    : buffer_pool_manager_(other.buffer_pool_manager_),
      schema_(other.schema_),
      log_manager_(other.log_manager_),
      lock_manager_(other.lock_manager_),
      record_now_(other.record_now_)
       {
  
  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_now_.GetRowId().GetPageId()))
      ->GetTuple(&record_now_, schema_, txn, lock_manager_);
      
  //i don't know whether the page is edited or not
  //except add a mark when * it
  buffer_pool_manager_->UnpinPage(record_now_.GetRowId().GetPageId(), true);
}

TableIterator::~TableIterator() {}

/*
inline bool TableIterator::operator==(const TableIterator &itr) const { return (this->record_id_now_ == itr.record_id_now_); }

inline bool TableIterator::operator!=(const TableIterator &itr) const {
  return (this->record_id_now_.Get() != itr.record_id_now_.Get());
}
*/

TableIterator& TableIterator::operator=(const TableIterator& other) {
  buffer_pool_manager_ = other.buffer_pool_manager_;
  schema_ = other.schema_;
  log_manager_ = other.log_manager_;
  lock_manager_ = other.lock_manager_;
  txn = other.txn;
  record_now_.SetRowId(other.record_now_.GetRowId());
  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(other.record_now_.GetRowId().GetPageId()))
      ->GetTuple(&record_now_, schema_, txn, lock_manager_);
  return *this;
}

const Row &TableIterator::operator*() {
  // ASSERT(false, "Not implemented yet.");
  return record_now_;
}

Row *TableIterator::operator->() { return &record_now_; }

TableIterator &TableIterator::operator++() {
  // point to the next slot
  RowId tmp_row = record_now_.GetRowId();
  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_now_.GetRowId().GetPageId()))
      ->GetNextTupleRid(tmp_row, &tmp_row);

  if (tmp_row.GetPageId() != INVALID_PAGE_ID) {
    record_now_.SetRowId(tmp_row);
    reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_now_.GetRowId().GetPageId()))
        ->GetTuple(&record_now_, schema_, txn, lock_manager_);
    buffer_pool_manager_->UnpinPage(record_now_.GetRowId().GetPageId(), true);
  } else {
    // try to get the first tuple in next page
    RowId old_row = record_now_.GetRowId();
    RowId tmp_row = record_now_.GetRowId();
    page_id_t the_page_id =
        reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(tmp_row.GetPageId()))->GetNextPageId();

    if (the_page_id == INVALID_PAGE_ID) {
      buffer_pool_manager_->UnpinPage(old_row.GetPageId(), true);
      record_now_.SetRowId(INVALID_ROWID);
      return *this;
    }

    reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(the_page_id))->GetFirstTupleRid(&tmp_row);
    record_now_.SetRowId(tmp_row);
    // get the tuple
    reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_now_.GetRowId().GetPageId()))
        ->GetTuple(&record_now_, schema_, txn, lock_manager_);

    buffer_pool_manager_->UnpinPage(old_row.GetPageId(), true);
    buffer_pool_manager_->UnpinPage(record_now_.GetRowId().GetPageId(), true);
  }
  return *this;
}

TableIterator TableIterator::operator++(int) { // point to the next slot
  TableIterator tmp(*this);
  // point to the next slot
  RowId tmp_row = record_now_.GetRowId();
  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_now_.GetRowId().GetPageId()))
      ->GetNextTupleRid(tmp_row, &tmp_row);

  if (tmp_row.GetPageId() != INVALID_PAGE_ID) {
    record_now_.SetRowId(tmp_row);
    reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_now_.GetRowId().GetPageId()))
        ->GetTuple(&record_now_, schema_, txn, lock_manager_);
    buffer_pool_manager_->UnpinPage(record_now_.GetRowId().GetPageId(), true);
  } else {
    // try to get the first tuple in next page
    RowId old_row = record_now_.GetRowId();
    RowId tmp_row = record_now_.GetRowId();
    page_id_t the_page_id = 
        reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(tmp_row.GetPageId()))->GetNextPageId();


    if (the_page_id==INVALID_PAGE_ID) {
      buffer_pool_manager_->UnpinPage(old_row.GetPageId(), true);
      record_now_.SetRowId(INVALID_ROWID);
      return TableIterator(tmp);
    } 


    reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(the_page_id))->GetFirstTupleRid(&tmp_row);
    record_now_.SetRowId(tmp_row);
    // get the tuple
    reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(record_now_.GetRowId().GetPageId()))
        ->GetTuple(&record_now_, schema_, txn, lock_manager_);

    buffer_pool_manager_->UnpinPage(old_row.GetPageId(), true);
    buffer_pool_manager_->UnpinPage(record_now_.GetRowId().GetPageId(), true);
  }
  return TableIterator(tmp);
}
