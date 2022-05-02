#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage *meta = (DiskFileMetaPage *)meta_data_;
  page_id_t bitmap_phy_id = 0;
  uint32_t inner_offset = 0;
  BitmapPage<PAGE_SIZE> page;
  memset(&page, 0, sizeof(page));

  //full
  if (meta->GetAllocatedPages() >= MAX_VALID_PAGE_ID)
    return INVALID_PAGE_ID;

  // edit the extent meta-data
  uint32_t i = 0;
  for (;i<MAX_VALID_EXTENT_ID;i++) {
    if (meta->extent_used_page_[i] == BITMAP_SIZE)
      continue;
    else
      break;
  }

  bitmap_phy_id = 1 + i * (BITMAP_SIZE+1);
  //new extent
  if (meta->num_extents_<=i) {
    WritePhysicalPage(bitmap_phy_id, (char*)&page);
  }
  ReadPhysicalPage(bitmap_phy_id, (char *)&page);
  page.AllocatePage(inner_offset);
  WritePhysicalPage(bitmap_phy_id, (char *)&page);
  //edit the file meta-data
  (meta->extent_used_page_[i])++;
  (meta->num_allocated_pages_)++;
  if (meta->num_extents_ <= i) meta->num_extents_++;

  return i * (BITMAP_SIZE) + inner_offset;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  DiskFileMetaPage *meta = (DiskFileMetaPage *)meta_data_;
  page_id_t bitmap_phy_id = 0;
  uint32_t inner_offset = 0;
  BitmapPage<PAGE_SIZE> page;

  // edit the extent meta-data
  int i = logical_page_id/BITMAP_SIZE;
  inner_offset = logical_page_id % BITMAP_SIZE;
  bitmap_phy_id = 1 + i * (BITMAP_SIZE + 1);
  ReadPhysicalPage(bitmap_phy_id, (char *)&page);
  page.DeAllocatePage(inner_offset);
  WritePhysicalPage(bitmap_phy_id, (char *)&page);
  // edit the file meta-data
  (meta->extent_used_page_[i])--;
  (meta->num_allocated_pages_)--;
  if (meta->extent_used_page_[i]==0) meta->num_extents_--;

}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  page_id_t bitmap_phy_id = 0;
  uint32_t inner_offset = 0;
  static BitmapPage<PAGE_SIZE> page;

  //read the extent meta-data
  int i = logical_page_id / BITMAP_SIZE;
  inner_offset = logical_page_id % BITMAP_SIZE;
  bitmap_phy_id = 1 + i * (BITMAP_SIZE + 1);
  ReadPhysicalPage(bitmap_phy_id, (char *)&page);
  return page.IsPageFree(inner_offset);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id + 2 + logical_page_id / BITMAP_SIZE;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}