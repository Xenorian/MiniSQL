#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t ofs = 0;
  // Write the MagicNum
  MACH_WRITE_UINT32(buf + ofs, INDEX_METADATA_MAGIC_NUM);
  ofs = ofs + 4;
  
  MACH_WRITE_UINT32(buf + ofs, index_id_);
  ofs = ofs + 4;

  MACH_WRITE_UINT32(buf + ofs, index_name_.length());
  ofs = ofs + 4;

  MACH_WRITE_STRING(buf + ofs, index_name_);
  ofs = ofs + index_name_.length();

  MACH_WRITE_UINT32(buf + ofs, index_id_);
  ofs = ofs + 4;

  MACH_WRITE_UINT32(buf + ofs, table_id_);
  ofs = ofs + 4;

  MACH_WRITE_UINT32(buf + ofs, key_map_.size());
  ofs = ofs + 4;

  uint32_t i;
  for (i = 0; i < key_map_.size(); i++) {
    MACH_WRITE_UINT32(buf + ofs, key_map_[i]);
    ofs = ofs + 4;
  }

  return ofs;
}

uint32_t IndexMetadata::GetSerializedSize() const { 
  return 24 + index_name_.length() + key_map_.size() * 4; 
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  uint32_t ofs = 0;

  if (index_meta != nullptr) {
    std::cerr << "Pointer to index_meta is not null in column deserialize." << std::endl;
  }
  if (buf == NULL) {
    return 0;
  }

  uint32_t Magic_Number = MACH_READ_UINT32(buf + ofs);
  if (Magic_Number != 344528) {
    std::cerr << "INDEX_METADATA_MAGIC_NUM does not match" << std::endl;
    return 0;
  } 
  else {
    ofs = ofs + 4;
  }

  index_id_t index_id = MACH_READ_UINT32(buf + ofs);
  ofs = ofs + 4;

  uint32_t i;

  uint32_t index_name_length = MACH_READ_UINT32(buf + ofs);
  ofs = ofs + 4;

  std::string index_name;
  for (i = 0; i < index_name_length; i++) {
    index_name.push_back(buf[ofs]);
    ofs = ofs + 1;
  }

  table_id_t table_id = MACH_READ_UINT32(buf + ofs);
  ofs = ofs + 4;

  uint32_t key_map_size = MACH_READ_UINT32(buf + ofs);
  ofs = ofs + 4;

  std::vector<uint32_t> key_map;
  for (i = 0; i < key_map_size; i++) {
    key_map.push_back(buf[ofs]);
    ofs = ofs + 4;
  }

  index_meta = ALLOC_P(heap, IndexMetadata)(index_id, index_name, table_id, key_map);

  return ofs;
}