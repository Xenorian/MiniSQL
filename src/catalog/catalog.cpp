#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t ofs = 0;
  // Write the MagicNum
  MACH_WRITE_UINT32(buf + ofs, CATALOG_METADATA_MAGIC_NUM);
  ofs = ofs + 4;

  MACH_WRITE_UINT32(buf + ofs, table_meta_pages_.size());
  ofs = ofs + 4;

  for (auto iter = table_meta_pages_.begin(); iter != table_meta_pages_.end(); iter++) {
    MACH_WRITE_UINT32(buf + ofs, iter->first);
    ofs = ofs + 4;
    MACH_WRITE_INT32(buf + ofs, iter->second);
    ofs = ofs + 4;
  }

  MACH_WRITE_UINT32(buf + ofs, index_meta_pages_.size());
  ofs = ofs + 4;

  for (auto iter = index_meta_pages_.begin(); iter != index_meta_pages_.end(); iter++) {
    MACH_WRITE_UINT32(buf + ofs, iter->first);
    ofs = ofs + 4;
    MACH_WRITE_INT32(buf + ofs, iter->second);
    ofs = ofs + 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  uint32_t ofs = 0;

  if (buf == NULL) {
    return 0;
  }

  uint32_t Magic_Number = MACH_READ_UINT32(buf + ofs);
  if (Magic_Number != 89849) {
    std::cerr << "CATALOG_METADATA_MAGIC_NUM does not match" << std::endl;
    return 0;
  } else {
    ofs = ofs + 4;
  }

  uint32_t i;
  // std::map<table_id_t, page_id_t> table_meta_pages;
  // std::map<index_id_t, page_id_t> index_meta_pages;
  CatalogMeta *my_catalogmeta = ALLOC_P(heap, CatalogMeta)();

  uint32_t table_page_size = MACH_READ_UINT32(buf + ofs);
  ofs = ofs + 4;

  for (i = 0; i < table_page_size; i++) {
    table_id_t tmp_table_id = MACH_READ_UINT32(buf + ofs);
    ofs = ofs + 4;
    page_id_t tmp_page_id = MACH_READ_INT32(buf + ofs);
    ofs = ofs + 4;
    my_catalogmeta->table_meta_pages_[tmp_table_id] = tmp_page_id;
  }

  uint32_t index_page_size = MACH_READ_UINT32(buf + ofs);
  ofs = ofs + 4;

  for (i = 0; i < index_page_size; i++) {
    index_id_t tmp_index_id = MACH_READ_UINT32(buf + ofs);
    ofs = ofs + 4;
    page_id_t tmp_page_id = MACH_READ_INT32(buf + ofs);
    ofs = ofs + 4;
    my_catalogmeta->index_meta_pages_[tmp_index_id] = tmp_page_id;
  }

  return my_catalogmeta;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  return 12 + table_meta_pages_.size() * 8 + index_meta_pages_.size() * 8;
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      heap_(new SimpleMemHeap()) {
  if (init) {  // the first time to create
    next_table_id_ = 0;
    next_index_id_ = 0;
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
  }
  else{
    Page* p = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    cout << "init: get page id: " << p->GetPageId() <<endl;
    char* t = p->GetData();
    catalog_meta_ = CatalogMeta::DeserializeFrom(t,heap_);
    catalog_meta_ = CatalogMeta::DeserializeFrom(t,heap_);

    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();

    for (auto it : catalog_meta_->table_meta_pages_) {
      if (it.second < 0) continue;
      TableMetadata *meta = nullptr;
      meta->DeserializeFrom(buffer_pool_manager->FetchPage(it.second)->GetData(), meta, heap_);
      TableInfo *tinfo = nullptr;
      tinfo = TableInfo::Create(heap_);
      TableHeap *table_heap =
          TableHeap::Create(buffer_pool_manager_, meta->GetSchema(), nullptr, log_manager_, lock_manager_, heap_);
      tinfo->Init(meta, table_heap);
      table_names_[meta->GetTableName()] = meta->GetTableId();
      tables_[meta->GetTableId()] = tinfo;
      index_names_.insert({meta->GetTableName(), std::unordered_map<std::string, index_id_t>()});
      buffer_pool_manager->UnpinPage(it.second, false);
    }

    for (auto it : catalog_meta_->index_meta_pages_) {
      if (it.second < 0) continue;
      IndexMetadata *meta = nullptr;
      IndexMetadata::DeserializeFrom(buffer_pool_manager->FetchPage(it.second)->GetData(), meta, heap_);
      TableInfo *tinfo = tables_[meta->GetTableId()];
      IndexInfo *index_info = IndexInfo::Create(heap_);
      index_info->Init(meta, tinfo, buffer_pool_manager_);  // segmentation
      index_names_[tinfo->GetTableName()][meta->GetIndexName()] = meta->GetIndexId();
      indexes_[meta->GetIndexId()] = index_info;
      buffer_pool_manager->UnpinPage(it.second, false);
    }

    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
}
CatalogManager::~CatalogManager() {
  catalog_meta_->SerializeTo((buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID))->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  delete heap_;
}
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if (table_names_.find(table_name) != table_names_.end()) {  // table exist
    return DB_TABLE_ALREADY_EXIST;
  }
  table_info = TableInfo::Create(heap_);
  page_id_t page_id;  //分配数据页
  Page *new_table_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->table_meta_pages_[next_table_id_] = page_id;

  TableHeap *table_heap =
      TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, table_info->GetMemHeap());
  TableMetadata *table_meta =
      TableMetadata::Create(next_table_id_, table_name, table_heap->GetFirstPageId(), schema, heap_);

  // cout << table_heap->GetFirstPageId() <<endl;

  table_info->Init(table_meta, table_heap);
  tables_[next_table_id_] = table_info;  // update tables_

  table_meta->SerializeTo(new_table_page->GetData());

  catalog_meta_->SerializeTo((buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID))->GetData());
  cout << "CreatTable Success, <tid, pid> = " << next_table_id_ << ", " << page_id << endl;

  table_names_[table_name] = next_table_id_++;  // update table_names_ & next_table_id_
  index_names_.insert({table_name, std::unordered_map<std::string, index_id_t>()});

  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->UnpinPage(page_id, true);

  if (table_meta != nullptr && table_heap != nullptr) return DB_SUCCESS;
  return DB_FAILED;

  // table_id_t table_id = next_table_id_++;
  // catalog_meta_->table_meta_pages_[table_id] = page_id;
  // catalog_meta_->table_meta_pages_[next_table_id_] = -1;
  // TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, page_id, schema, heap_);
  // TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,schema, txn, log_manager_, lock_manager_, heap_);
  // table_info = TableInfo::Create(heap_);
  // table_info->Init(table_meta, table_heap);

  // table_names_[table_name] = table_id;
  // tables_[table_id] = table_info;
  // index_names_.insert({table_name, std::unordered_map<std::string, index_id_t>()});
  // ///将tablematedata写入数据页
  // auto len = table_meta->GetSerializedSize();
  // char meta[len+1];
  // table_meta->SerializeTo(meta);
  // char* p = new_table_page->GetData();
  // memcpy(p,meta,len);

  // len = catalog_meta_->GetSerializedSize();
  // char cmeta[len+1];
  // catalog_meta_->SerializeTo(cmeta);
  // p = buffer_pool_manager_->FetchPage(0)->GetData();
  // memset(p, 0, PAGE_SIZE);
  // memcpy(p,cmeta,len);
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto tmp = table_names_.find(table_name);
  if (tmp != table_names_.end()) {  // find the table
    auto table_id = tmp->second;
    cout << "get_table_id: " << table_id << endl;
    table_info = tables_[table_id];
    return DB_SUCCESS;
  }
  return DB_TABLE_NOT_EXIST;  // not find
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  // if(!tables_.size()){ //no tables
  //   return DB_TABLE_NOT_EXIST;
  // }
  for (auto i : tables_) {
    tables.push_back(i.second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // ASSERT(false, "Not Implemented yet");
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto tmp_index = index_names_[table_name];
  if (tmp_index.find(index_name) != tmp_index.end()) {
    return DB_INDEX_ALREADY_EXIST;
  }

  TableInfo *table_info;
  auto err = GetTable(table_name, table_info);  // get table_info
  if (err) return err;                          // error

  std::vector<uint32_t> key_map;
  for (auto i : index_keys) {
    uint32_t t;
    if (e == DB_COLUMN_NAME_NOT_EXIST) return e;  // DB_COLUMN_NAME_NOT_EXIST
    if(e) return e; //DB_COLUMN_NAME_NOT_EXIST
    key_map.push_back(t);
  }

  IndexMetadata *index_meta_data_ptr =
      IndexMetadata::Create(next_index_id_, index_name, table_names_[table_name], key_map, heap_);

  index_info->Init(index_meta_data_ptr,table_info,buffer_pool_manager_);
  //put all the info into the new index
  index_info->Init(index_meta_data_ptr,table_info,buffer_pool_manager_);

  std::vector<Field> tmp_fields;
  for (auto i = table_info->GetTableHeap()->Begin(nullptr); i != table_info->GetTableHeap()->End(); i++) {
    tmp_fields.clear();
    //get the key_field
    for (auto j : key_map) tmp_fields.push_back(*(i->GetField(j)));
    //ctor the key_row
    Row tmp_row = Row(tmp_fields);
    index_info->GetIndex()->InsertEntry(tmp_row, i->GetRowId(), nullptr);
  }

  //将index的信息写入index_roots_page
  page_id_t page_id;
  index_meta_data_ptr->SerializeTo(buffer_pool_manager_->NewPage(page_id)->GetData());
  catalog_meta_->index_meta_pages_[next_index_id_] = page_id;  // update catalog_meta_
  catalog_meta_->SerializeTo((buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID))->GetData());
  cout << "CreatIndex Success, <tid, pid> = " << next_index_id_ << ", " << page_id << endl;
  indexes_[next_index_id_] = index_info;                    // update indexes_
  index_names_[table_name][index_name] = next_index_id_++;  // update index_names_ & next_index_id_

  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->UnpinPage(page_id, true);

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  auto tmp_table = index_names_.find(table_name);
  if (tmp_table != index_names_.end()) {  // find the table
    auto tmp_index_table = tmp_table->second;
    auto tmp_index_id = tmp_index_table.find(index_name);
    if (tmp_index_id == tmp_index_table.end()) {
      return DB_INDEX_NOT_FOUND;
    }
    auto index_id = tmp_index_id->second;
    auto tmp = indexes_.find(index_id);
    if (tmp == indexes_.end()) {
      return DB_INDEX_NOT_FOUND;
    }
    index_info = tmp->second;
    return DB_SUCCESS;
  }
  return DB_TABLE_NOT_EXIST;  // not find
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  auto tmp_table = index_names_.find(table_name);
  if (tmp_table != index_names_.end()) {  // find the table
    auto tmp_index_table = tmp_table->second;
    for (auto i : tmp_index_table) {
      indexes.push_back(indexes_.find(i.second)->second);
    }
    return DB_SUCCESS;
  }
  return DB_TABLE_NOT_EXIST;  // not find
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  auto tmp = table_names_.find(table_name);
  if (tmp != table_names_.end()) {  // find the table
    // auto table_id = tmp->second;
    for(auto i: index->second){ //delete indexes_
      DropIndex(table_name, i.first);
       //delete the data
    //delete metapage
    buffer_pool_manager_->DeletePage((catalog_meta_->table_meta_pages_[tmp->second]));
    catalog_meta_->table_meta_pages_.erase(tmp->second);
    // delete the table in disk
    tables_[tmp->second]->GetTableHeap()->FreeHeap();
    //delete metadata
    tables_.erase(tmp->second);      // delete tables_
    table_names_.erase(table_name); //delete table_names_
    index_names_.erase(table_name); //delete index_names_
    tables_.erase(tmp->second); //delete tables_
    return DB_SUCCESS;
  } else
    return DB_TABLE_NOT_EXIST;  // not find
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  auto tmp_table = index_names_.find(table_name);
  if (tmp_table != index_names_.end()) {  // find the table
    auto tmp_index_table = tmp_table->second;
    auto tmp_index_id = tmp_index_table.find(index_name);
    if (tmp_index_id == tmp_index_table.end()) {
      return DB_INDEX_NOT_FOUND;
    }
    //1. delete index metapage
    buffer_pool_manager_->DeletePage((catalog_meta_->index_meta_pages_[index_id]));
    catalog_meta_->index_meta_pages_.erase(index_id);
    //2. delete index itself
    indexes_[index_id]->GetIndex()->Destroy();
    //3. delete record

    indexes_.erase(index_id);
    tmp_index_table.erase(index_name);
    return DB_SUCCESS;
  } else
    return DB_TABLE_NOT_EXIST;  // not find
}

dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  //新建写入内存的table_info
  auto *table_info = TableInfo::Create(heap_);
  // table_meta反序列化存储
  TableMetadata *table_meta = nullptr;
  TableMetadata::DeserializeFrom(buffer_pool_manager_->FetchPage(page_id)->GetData(), table_meta,
                                 table_info->GetMemHeap());
  ASSERT(table_meta != nullptr, "TABLEINFO INIT ERROR");
  //根据table_meta信息生成table_heap
  auto *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(),
                                       log_manager_, lock_manager_, table_info->GetMemHeap());
  //初始化table_info并插入catalogManager
  table_info->Init(table_meta, table_heap);
  tables_[table_id] = table_info;
  table_names_[table_info->GetTableName()] = table_info->GetTableId();
  /* thus table_meta and table_heap are created by table_info */

  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  //新建写入内存的index_info
  auto *index_info = IndexInfo::Create(heap_);
  // IndexMeta反序列化
  IndexMetadata *index_meta = nullptr;
  IndexMetadata::DeserializeFrom(buffer_pool_manager_->FetchPage(page_id)->GetData(), index_meta,
                                 index_info->GetMemHeap());
  ASSERT(index_meta != nullptr, "INDEXINFO INIT ERROR");
  //初始化index_info并插入catalogManager
  index_info->Init(index_meta, this->tables_[index_meta->GetTableId()], buffer_pool_manager_);
  indexes_[index_id] = index_info;
  std::unordered_map<std::string, index_id_t> temp;
  temp[index_info->GetIndexName()] = index_id;
  index_names_[index_info->GetTableInfo()->GetTableName()] = temp;

  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto tmp = tables_.find(table_id);
  if (tmp != tables_.end()) {
    table_info = tmp->second;
    return DB_SUCCESS;
  }
  return DB_TABLE_NOT_EXIST;
}
