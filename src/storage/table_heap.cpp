# include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  for (auto iter = Begin(txn); iter != End(); iter++) {
    
  }
  return false;
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

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  return false;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  // lock_manager_->Unlock(txn, rid);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
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
  auto cur_page = first_page_id_; 
  while(cur_page != INVALID_PAGE_ID){
    ;
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage((row->GetRowId()).GetPageId()));
  if(page != nullptr){ // CANNOT find the tuple with RowId == row.rid_
    return false;
  }
  //read the tuple
  page->RLatch();
  bool rst = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return rst;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  RowId id = INVALID_ROWID;
  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_))->GetFirstTupleRid(&id);
  return TableIterator(buffer_pool_manager_, schema_, log_manager_, lock_manager_,id);
}

TableIterator TableHeap::End() {
  RowId id = INVALID_ROWID;
  return TableIterator(buffer_pool_manager_, schema_, log_manager_, lock_manager_, id);
}
