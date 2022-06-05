#include "storage/table_heap.h"
#include "page/table_page.h"

page_id_t TableHeap::AllocateNewTablePage(page_id_t last_page_id, BufferPoolManager *buffer_pool_manager_, Transaction *txn,
                          LockManager *lock_manager, LogManager *log_manager) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  TablePage *new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  new_page->Init(new_page_id, last_page_id, log_manager, txn);
  new_page->SetNextPageId(INVALID_PAGE_ID);
  ((TablePage *)buffer_pool_manager_->FetchPage(last_page_id))->SetNextPageId(new_page_id);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_page_id;
}

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  //first edition. not take the transaction into consideration
  //first edition. suppose one record can be stored into one page
  
  //if the space is not enough, allocate another page
  //else. the tuple is inserted
  page_id_t page_old = INVALID_PAGE_ID;
  page_id_t page_now = first_page_id_;
  while (page_now!=INVALID_PAGE_ID) {
    if (reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_now))
            ->InsertTuple(row, schema_, txn, lock_manager_, log_manager_) == false) {
      page_old = page_now;
      page_now = ((TablePage *)buffer_pool_manager_->FetchPage(page_now))->GetNextPageId();
      buffer_pool_manager_->UnpinPage(page_old, false);
    }
    else {
      //buffer_pool_manager_->UnpinPage(page_now, true);
      break;
    }
  }

  //allocate new page
  if (page_now==INVALID_PAGE_ID) {
    page_now = AllocateNewTablePage(page_old, buffer_pool_manager_, txn, lock_manager_, log_manager_);
    if (((TablePage *)buffer_pool_manager_->FetchPage(page_now))
            ->InsertTuple(row, schema_, txn, lock_manager_, log_manager_) == false) {
      buffer_pool_manager_->UnpinPage(page_old, false);
      return false;
    }
  }

  buffer_pool_manager_->UnpinPage(page_now, true);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  Row old_row(rid);

  if (((TablePage *)buffer_pool_manager_->FetchPage(rid.GetPageId()))
          ->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_) == false) {

    MarkDelete(rid, txn); //this will call unpin
    return InsertTuple(row, txn); // this will call unpin
  }

  //update success
  row.SetRowId(rid);
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
  return true;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  TablePage *the_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // Step2: Delete the tuple from the page.
  the_page->ApplyDelete(rid, txn, log_manager_);

  /*
  * //not to delete the page
  // Step3: If the page is empty, delete the page.
  RowId test_rid;
  bool deleted = false;

  the_page->GetFirstTupleRid(&test_rid);
  if (test_rid == INVALID_ROWID) {
    TablePage *pre_page = nullptr;
    if (the_page->GetPrevPageId() != INVALID_PAGE_ID)
      pre_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(the_page->GetPrevPageId()));
    TablePage *next_page = nullptr;
    if (the_page->GetNextPageId() != INVALID_PAGE_ID)
      next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(the_page->GetNextPageId()));
    
    //if this page is the first page. not to delete it
    if (pre_page == nullptr) {
        //do nothing
    } else {
      if (next_page == nullptr) {
        pre_page->SetNextPageId(INVALID_PAGE_ID);
      } else {
        pre_page->SetNextPageId(next_page->GetPageId());
      }

      buffer_pool_manager_->UnpinPage(the_page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(the_page->GetPageId());
      deleted = true;
    }
    if (pre_page != nullptr) buffer_pool_manager_->UnpinPage(pre_page->GetPageId(), true);
    if (next_page != nullptr) buffer_pool_manager_->UnpinPage(next_page->GetPageId(), true);
  }
  if (deleted == false) buffer_pool_manager_->UnpinPage(the_page->GetPageId(), true);
  */

}


void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  page_id_t page_now = first_page_id_;
  page_id_t page_old = INVALID_PAGE_ID;
  TablePage *the_page = nullptr;
  while (page_now != INVALID_PAGE_ID) {
    page_old = page_now;
    the_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_old));
    page_now = the_page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page_old, true);
    buffer_pool_manager_->DeletePage(page_old);
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  TablePage *tp_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  tp_ptr->GetTuple(row, schema_, txn, lock_manager_);
  //will not edit the page
  buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
  return true;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  RowId tmp = INVALID_ROWID;
  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_))->GetFirstTupleRid(&tmp);
  if (tmp.GetPageId() != INVALID_PAGE_ID)
    buffer_pool_manager_->UnpinPage(tmp.GetPageId(), false);
  return TableIterator(tmp, buffer_pool_manager_, schema_, log_manager_, lock_manager_);
}

TableIterator TableHeap::End() {
  RowId tmp = INVALID_ROWID;
  return TableIterator(tmp, buffer_pool_manager_, schema_, log_manager_, lock_manager_);
}
