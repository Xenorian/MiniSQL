#include "record/schema.h"
#include <iostream>

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  //magic_num
  char *tmp = buf;
  MACH_WRITE_UINT32(tmp, SCHEMA_MAGIC_NUM);
  tmp += sizeof(uint32_t);
  //vector_size
  MACH_WRITE_UINT32(tmp, columns_.size());
  tmp += sizeof(uint32_t);
  //column_data
  size_t i = 0;
  for (; i < columns_.size(); i++) {
    columns_[i]->SerializeTo(tmp);
    tmp += columns_[i]->GetSerializedSize();
  }

  return tmp - buf;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t columns_size = 0;
  size_t i = 0;
  for (; i < columns_.size(); i++) columns_size += columns_[i]->GetSerializedSize();
  return sizeof(uint32_t) * 2 + columns_size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
  // magic_num
  char *tmp = buf;
  if (MACH_READ_UINT32(tmp) != SCHEMA_MAGIC_NUM) {
    std::cerr << "Magic number vertification failed" << std::endl;
  }
  tmp += sizeof(uint32_t);
  // vector_size
  uint32_t vector_size = MACH_READ_UINT32(tmp);
  tmp += sizeof(uint32_t);
  // column_data
  void *mem = heap->Allocate(sizeof(Schema));
  std::vector<Column *> columns;
  size_t i = 0;
  for (; i < vector_size; i++) {
    columns.push_back(NULL);
    Column::DeserializeFrom(tmp, (columns[i]), heap);
    tmp += columns[i]->GetSerializedSize();
  }
  schema = new (mem) Schema(columns);

  return tmp - buf;
}