#include "record/row.h"
#include <iostream>
/**
 *  Row format:
 * -------------------------------------------
 * | Header | Field | ... | Field-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | Field Nums | bool * n |
 * -------------------------------------------
 *
 *
 */

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  char *tmp = buf;
  //magic_num
  MACH_WRITE_UINT32(tmp, ROW_MAGIC_NUM);
  tmp += sizeof(uint32_t);
  //header
  MACH_WRITE_UINT32(tmp, fields_.size());
  tmp += sizeof(uint32_t);
  size_t i = 0;

  for (i = 0; i < fields_.size(); i++) {
    MACH_WRITE_TO(bool, tmp, fields_[i]->IsNull());
    tmp += sizeof(bool);
  }
  //fields
  for (i = 0; i < fields_.size(); i++) {
    if (fields_[i]->IsNull() == false) {
      fields_[i]->SerializeTo(tmp);
      tmp += fields_[i]->GetSerializedSize();
    }
  }

  return tmp - buf;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
  char *tmp = buf;
  // magic_num
  if (MACH_READ_UINT32(tmp) != ROW_MAGIC_NUM) std::cerr << "Magic Num vertification failed" << std::endl;
  tmp += sizeof(uint32_t);
  // header
  uint32_t size = MACH_READ_UINT32(tmp);
  tmp += sizeof(uint32_t);
  size_t i = 0;
  bool *null_map = static_cast<bool *>((heap_->Allocate(sizeof(bool[size]))));

  for (i = 0; i < size; i++) {
    null_map[i] = MACH_READ_FROM(bool, tmp);
    tmp += sizeof(bool);
  }
  // fields
  for (i = fields_.size(); i < schema->GetColumnCount();i++) {
    fields_.push_back(nullptr);
  }

  for (i = 0; i < schema->GetColumnCount(); i++) {
    if (null_map[i] == false) {
      fields_[i]->DeserializeFrom(tmp, schema->GetColumn(i)->GetType(), &(fields_[i]), false, heap_);
      tmp += fields_[i]->GetSerializedSize();
    } else {
      void *mem = heap_->Allocate(sizeof(Field));
      new (mem) Field(schema->GetColumn(i)->GetType());
      fields_[i] = (Field *)mem;
    }
  }

  return tmp - buf;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
  uint32_t ret_val = sizeof(uint32_t) * 2 + sizeof(bool) * fields_.size();
  size_t i = 0;
  for (; i < fields_.size();i++) {
    if (fields_[i]->IsNull() == false) ret_val += fields_[i]->GetSerializedSize();
  }
  return ret_val;
}
