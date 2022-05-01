#include "page/bitmap_page.h"

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (next_free_page_ >= GetMaxSupportedSize()) return false;  // full

  if (GetBit(next_free_page_) == 1) return false;  // system wrong

  page_offset = next_free_page_;  // get the free page
  EditBit(page_offset, 1);

  page_allocated_++;
  next_free_page_ = GetNextFreePage();  // find next free page
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (page_offset >= GetMaxSupportedSize()) return false;  // out of range

  if (GetBit(page_offset) == 0) return false;  // no allocation

  EditBit(page_offset, 0);

  page_allocated_--;
  next_free_page_ = page_offset;
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (GetBit(page_offset) == 0)
    return true;
  else
    return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::GetBit(uint32_t page_offset) const {
  int byte = page_offset / 8;
  int remain_bit = page_offset % 8;
  bool ret_val = (((uint8_t)bytes[byte]) >> (remain_bit)) & 0x01;
  return ret_val;
}

template <size_t PageSize>
void BitmapPage<PageSize>::EditBit(uint32_t page_offset, bool value) {
  static uint8_t true_mask[8] = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};
  static uint8_t false_mask[8] = {0xFE, 0xFD, 0xFB, 0xF7, 0xEF, 0xDF, 0xBF, 0x7F};
  int byte = page_offset / 8;
  int remain_bit = page_offset % 8;
  if (value == 0)
    bytes[byte] = bytes[byte] & false_mask[remain_bit];
  else
    bytes[byte] = bytes[byte] | true_mask[remain_bit];
}

template <size_t PageSize>
size_t BitmapPage<PageSize>::GetNextFreePage() const {
  size_t i = 0;
  size_t offset = 0;
  uint8_t mask = 1;
  for (i = 0; i < MAX_CHARS; i++) {
    if (bytes[i] == 0xFF) continue;

    while (1) {
      if ((bytes[i] & mask) == 0) return i * 8 + offset;
      offset++;
      mask = mask << 1;
    }
  }

  return GetMaxSupportedSize();
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;