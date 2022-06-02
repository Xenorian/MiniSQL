#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"
#include <iostream>

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }

}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  frame_id_t new_frame_id = 0;
  // 1.1    If P exists, pin it and return it immediately.
  if(page_table_.count(page_id)==1) {
    replacer_->Pin(page_id);
    if (pages_[(*page_table_.find(page_id)).second].pin_count_ < 0) std::cerr << "pin count < 0" << std::endl;
    pages_[(*page_table_.find(page_id)).second].pin_count_ = 1;
    return pages_ + (*page_table_.find(page_id)).second;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if(free_list_.size()!=0) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&new_frame_id);
  }
  // 2.     If R is dirty, write it back to the disk.
  if (pages_[new_frame_id].IsDirty() == 1) disk_manager_->WritePage(new_frame_id, pages_[new_frame_id].data_);
  // 3.     Delete R from the page table and insert P.
  if (page_table_.find(pages_[new_frame_id].page_id_)!=page_table_.end())
    page_table_.erase(page_table_.find(pages_[new_frame_id].page_id_));

  page_table_.insert(make_pair(page_id, new_frame_id));
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pages_[new_frame_id].is_dirty_ = 0;
  pages_[new_frame_id].page_id_ = page_id;
  pages_[new_frame_id].pin_count_ = 0;

  disk_manager_->ReadPage(page_id, pages_[new_frame_id].data_);
  return pages_ + new_frame_id;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  int all_pin = 0;
  if (free_list_.size() != 0) all_pin++;
  if (replacer_->Size() != 0) all_pin++;
  if (all_pin == 0) return nullptr;

  page_id = disk_manager_->AllocatePage();
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t new_frame_id = 0;
  if (free_list_.size() != 0) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&new_frame_id);
    if (pages_[new_frame_id].IsDirty() == 1) disk_manager_->WritePage(new_frame_id, pages_[new_frame_id].data_);
    page_table_.erase(page_table_.find(pages_[new_frame_id].page_id_));
  }
  
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  pages_[new_frame_id].ResetMemory();
  pages_[new_frame_id].pin_count_ = 1;
  pages_[new_frame_id].is_dirty_ = 0;
  pages_[new_frame_id].page_id_ = page_id;

  page_table_.insert(make_pair(page_id, new_frame_id));
  //replacer_->Unpin(new_frame_id);
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return pages_ + new_frame_id;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  disk_manager_->DeAllocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  unordered_map<page_id_t, frame_id_t>::iterator the_frame = page_table_.find(page_id);
  // 1.   If P does not exist, return true.
  if (the_frame == page_table_.end()) return true;
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (pages_[the_frame->second].pin_count_ != 0) {
      //for test
    std::cerr << "pined cannot delete" << endl;
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  pages_[the_frame->second].ResetMemory();
  free_list_.push_back(the_frame->second);
  page_table_.erase(page_id);
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  replacer_->Unpin((*page_table_.find(page_id)).second);
  pages_[(*page_table_.find(page_id)).second].is_dirty_ |= is_dirty;
  pages_[(*page_table_.find(page_id)).second].pin_count_ = 0;
  return true;
}
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if (pages_[(*page_table_.find(page_id)).second].IsDirty() == true) {
    disk_manager_->WritePage(page_id, pages_[(*page_table_.find(page_id)).second].data_);
  }
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}
 
// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}