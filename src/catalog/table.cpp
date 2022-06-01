#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t ofs = 0;
  // Write the MagicNum
  MACH_WRITE_UINT32(buf + ofs, TABLE_METADATA_MAGIC_NUM);
  ofs = ofs + 4;

  MACH_WRITE_UINT32(buf + ofs, table_id_);
  ofs = ofs + 4;

  MACH_WRITE_UINT32(buf + ofs, table_name_.length());
  ofs = ofs + 4;

  MACH_WRITE_STRING(buf + ofs, table_name_);
  ofs = ofs + table_name_.length();

  MACH_WRITE_INT32(buf + ofs, root_page_id_);
  ofs = ofs + 4;

  ofs = ofs + schema_->SerializeTo(buf + ofs);

  return ofs;
}

uint32_t TableMetadata::GetSerializedSize() const { 
  return 16 + table_name_.length() + schema_->GetSerializedSize(); 
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  uint32_t ofs = 0;

  if (table_meta != nullptr) {
    std::cerr << "Pointer to table_meta is not null in column deserialize." << std::endl;
  }
  if (buf == NULL) {
    return 0;
  }

  uint32_t Magic_Number = MACH_READ_UINT32(buf + ofs);
  if (Magic_Number != 344528) {
    std::cerr << "TABLE_METADATA_MAGIC_NUM does not match" << std::endl;
    return 0;
  } else {
    ofs = ofs + 4;
  }

  index_id_t table_id = MACH_READ_UINT32(buf + ofs);
  ofs = ofs + 4;

  uint32_t i;

  uint32_t table_name_length = MACH_READ_UINT32(buf + ofs);
  ofs = ofs + 4;

  std::string table_name;
  for (i = 0; i < table_name_length; i++) {
    table_name.push_back(buf[ofs]);
    ofs = ofs + 1;
  }

  table_id_t root_page_id_ = MACH_READ_INT32(buf + ofs);
  ofs = ofs + 4;

  Schema *schema;
  ofs = ofs + Schema::DeserializeFrom(buf + ofs, schema, heap);

  table_meta = ALLOC_P(heap, TableMetadata)(table_id, table_name, root_page_id_, schema);

  return ofs;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
