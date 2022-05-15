# include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  auto cur_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_)); // get the pointer to the first page
  if (cur_page == nullptr) {
    return false;
  }
  // Insert into the first page with enough space. If no such page exists, create a new page and insert into that.
  cur_page->WLatch();
  
  while (!cur_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) { 
    // cannot insert into the current page
    auto next_page_id = cur_page->GetNextPageId(); //get id of the next page
    if (next_page_id != INVALID_PAGE_ID) {
      // Unlatch and unpin the current page
      cur_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
      // turn to the next page
      cur_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
      cur_page->WLatch();
    } 
    else { // no such page exist: create a new page
      auto new_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      // If we could not create a new page
      if (new_page == nullptr) {
        cur_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
        return false;
      }
      // Initialize the new page and insert
      new_page->WLatch();
      cur_page->SetNextPageId(next_page_id);
      new_page->Init(next_page_id, cur_page->GetTablePageId(), log_manager_, txn);
      cur_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), true);
      cur_page = new_page;
    }
  }
  cur_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), true);
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
  // find the page which contains the tuple
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // cannot be found
  if (page == nullptr) {
    return false;
  }
  // update the tuple
  Row old_tuple(rid);
  page->WLatch();
  int is_updated = page->UpdateTuple(row, &old_tuple, schema_, txn, lock_manager_, log_manager_);  // -1:invalid; 0:space not enough; 1:success
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), is_updated>0);
  if(is_updated){ //-1 or 1
    return is_updated;
  }
  else{ //delete & insert
    MarkDelete(rid, txn); //delete
    return InsertTuple(row, txn);
  }
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
  while (cur_page != INVALID_PAGE_ID) {
    auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page))->GetNextPageId();
    buffer_pool_manager_->DeletePage(cur_page);
    cur_page = next_page;
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage((row->GetRowId()).GetPageId()));
  if(page == nullptr){ // CANNOT find the tuple with RowId == row.rid_
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
