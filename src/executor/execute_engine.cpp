#include "executor/execute_engine.h"
#include "glog/logging.h"
#include <string>

ExecuteEngine::ExecuteEngine() {}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
  /*#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
  #endif
  return DB_FAILED;*/
  std::string database_name = ast->child_->val_;

  //exist or not
  ifstream test_file;
  test_file.open(database_name, ios::in);

  if (test_file.good()) {
    //该database已经存在
    test_file.close();
    return DB_FAILED;
  } else {
    dbs_[database_name] = new DBStorageEngine(database_name, true);
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
  /*#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
  #endif
  return DB_FAILED;*/
  std::string database_name = ast->child_->val_;
  if (dbs_.find(database_name) == dbs_.end()) {
    //该database不存在
    return DB_FAILED;
  } else {
    auto iter = dbs_.find(database_name);
    dbs_.erase(iter);
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  return DB_FAILED;
  //不知道show到哪里去
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
  /*#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
  #endif
  return DB_FAILED;*/ 
  std::string database_name = ast->child_->val_;

  auto iter = dbs_.find(database_name);
  if (iter == dbs_.end()) {
    //test
    ifstream test_file;
    test_file.open(database_name, ios::in);

    if (test_file.good()) {
      //该database已经存在
      test_file.close();
      dbs_[database_name] = new DBStorageEngine(database_name, false);
      current_db_ = database_name;
      return DB_SUCCESS;
    } else {
      //not exist
      return DB_FAILED;
    }
  } else {
    //already opened
    current_db_ = iter->first;
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  return DB_FAILED;
  //不知道show到哪里去
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
  /*#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
  #endif
  return DB_FAILED;*/
  if (current_db_ == "") {
    //未选择database
    return DB_FAILED;
  }
  DBStorageEngine *databese_now = dbs_[current_db_];
  std::string table_name = ast->child_->val_;
  TableInfo *my_tableinfo = nullptr;
  /*if (databese_now->catalog_mgr_->GetTable(table_name, my_tableinfo) == DB_SUCCESS) {
    //该table已存在
    return DB_FAILED;
  }*/
  pSyntaxNode definition = ast->child_->next_->child_;
  vector<Column *> column_definition;
  vector<std::string> primary_keys;
  uint32_t index = 0;
  while (definition != NULL) {
    std::string column_name;
    TypeId type;
    uint32_t length = 0;
    bool nullable = true;
    bool unique = false;
    std::string special;
    if (definition->val_ != NULL) {
      special = definition->val_;
    }
    if (special == "primary keys") {
      pSyntaxNode keys = definition->child_;
      while (keys != NULL) {
        column_name = keys->val_;
        primary_keys.push_back(column_name);
        keys = keys->next_;
      }
    } else {
      if (special == "unique") {
        unique = true;
      }
      column_name = definition->child_->val_;
      std::string my_type = definition->child_->next_->val_;
      if (my_type == "int") {
        type = kTypeInt;
      } else if (my_type == "float") {
        type = kTypeFloat;
      } else if (my_type == "char") {
        type = kTypeChar;
        std::string char_length = definition->child_->next_->child_->val_;
        for (uint32_t i = 0; i < char_length.length(); i++) {
          if (char_length[i] < '0' || char_length[i] > '9') {
            //非法输入
            return DB_FAILED;
          }
          length = length * 10 + char_length[i] - '0';
        }
      } else {
        type = kTypeInvalid;
      }
      if (type == kTypeChar) {
        Column tmp(column_name, type, length, index, nullable, unique);
        column_definition.push_back(&tmp);
      } else {
        Column tmp(column_name, type, index, nullable, unique);
        column_definition.push_back(&tmp);
      }
    }
    definition = definition->next_;
    index++;
  }
  TableSchema my_schema(column_definition);
  // 主键现在还不能传
  return databese_now->catalog_mgr_->CreateTable(table_name, &my_schema, nullptr, my_tableinfo);
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
  /*#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
  #endif
  return DB_FAILED;*/
  if (current_db_ == "") {
    //未选择database
    return DB_FAILED;
  }
  std::string table_name = ast->child_->val_;
  DBStorageEngine *databese_now = dbs_[current_db_];
  return databese_now->catalog_mgr_->DropTable(table_name);
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  return DB_FAILED;
  //不知道show到哪里去
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
  /*#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
  #endif
  return DB_FAILED;*/
  if (current_db_ == "") {
    //未选择database
    return DB_FAILED;
  }
  DBStorageEngine *databese_now = dbs_[current_db_];
  std::string index_name = ast->child_->val_;
  std::string table_name = ast->child_->val_;
  TableInfo *my_table_info = nullptr;
  IndexInfo *my_index_info = nullptr;
  if (databese_now->catalog_mgr_->GetTable(table_name, my_table_info) == DB_FAILED) {
    //该table不存在
    return DB_FAILED;
  }
  if (databese_now->catalog_mgr_->GetIndex(table_name, index_name, my_index_info) == DB_SUCCESS) {
    //该table的index已存在
    return DB_FAILED;
  }
  vector<std::string> index_keys;
  pSyntaxNode indexes = ast->child_->next_->next_->child_;
  while (indexes != NULL) {
    std::string tmp_index_key = indexes->val_;
    index_keys.push_back(tmp_index_key);
    indexes = indexes->next_;
  }
  IndexInfo *index_info = nullptr;
  return databese_now->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, nullptr, index_info);
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
  /*#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
  #endif
  return DB_FAILED;*/
  if (current_db_ == "") {
    //未选择database
    return DB_FAILED;
  }
  DBStorageEngine *databese_now = dbs_[current_db_];
  std::string index_name = ast->child_->val_;
  std::string table_name;
  //只给index_name,去找table_name还不会
  if (0) {
    //找不到该index
    return DB_FAILED;
  }
  return databese_now->catalog_mgr_->DropIndex(table_name, index_name);
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
  /*#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropIndex" << std::endl;
    #endif
    return DB_FAILED;*/
  if (current_db_ == "") {
    //未选择database
    return DB_FAILED;
  }
  DBStorageEngine *databese_now = dbs_[current_db_];
  //符合条件的row
  vector<Row> select_rows;
  pSyntaxNode select_type = ast->child_;
  bool all_columns = false;
  vector<string> select_column_name;
  //select * 模式
  if (select_type->type_ == kNodeAllColumns) {
    all_columns = true;
  } 
  //select xxx, xxx, ... 模式
  else {
    pSyntaxNode select_column_node = select_type->child_;
    while (select_column_node != NULL) {
      string tmp_column_name = select_column_node->val_;
      select_column_name.push_back(tmp_column_name);
      select_column_node = select_column_node->next_;
    }
  }
  //select ... from xxx
  std::string table_name = select_type->next_->val_;
  TableInfo *my_table_info = nullptr;
  if (databese_now->catalog_mgr_->GetTable(table_name, my_table_info) == DB_FAILED) {
    //找不到该table
    return DB_FAILED;
  }
  vector<Column*> my_columns = my_table_info->GetSchema()->GetColumns();
  //select ... from xxx后面没有where
  if (select_type->next_->next_ == NULL) {
    for (auto iter = my_table_info->GetTableHeap()->Begin(nullptr); iter != my_table_info->GetTableHeap()->End();
         iter++) {
      Row my_row = *iter;
      select_rows.push_back(my_row);
    }
  } 
  //select * from xxx后面有where
  else {
    pSyntaxNode condition = select_type->next_->next_->child_;
    std::string what_char = condition->val_;
    pSyntaxNode compare = condition;
    //有and和or
    if (what_char == "and" || what_char == "or") {
      while (1) {
        std::string tmp_val = compare->val_;
        if (tmp_val != "and" && tmp_val != "or") {
          break;
        }
        compare = compare->child_;
      }
      // =，>，<等符号开始出现的地方
      pSyntaxNode compare_start = compare;
      for (auto iter = my_table_info->GetTableHeap()->Begin(nullptr); iter != my_table_info->GetTableHeap()->End();
           iter++) {
        Row my_row = *iter;
        //每一个row是否满足条件
        bool first_flag = true;
        bool all_satisfy;
        bool this_satisfy;
        while (condition != compare_start && compare != NULL) {
          std::string tmp_condition = condition->val_;
          std::string tmp_compare = compare->val_;
          string compare_column_name = compare->child_->val_;
          string expect_val = compare->child_->next_->val_;
          TypeId tmp_type;
          uint32_t j;
          for (j = 0; j < my_columns.size(); j++) {
            if (compare_column_name == my_columns[j]->GetName()) {
              tmp_type = my_columns[j]->GetType();
              break;
            }
          }
          Field *real_field = my_row.GetField(j);
          Field *tmp_field;
          if (tmp_type == kTypeInt) {
            int32_t tmp_compare_val = stoi(expect_val);
            Field tmp_tmp_field(kTypeInt, tmp_compare_val);
            tmp_field = &tmp_tmp_field;
          } else if (tmp_type == kTypeFloat) {
            int32_t tmp_compare_val = stof(expect_val);
            Field tmp_tmp_field(kTypeFloat, tmp_compare_val);
            tmp_field = &tmp_tmp_field;
          } else if (tmp_type == kTypeChar) {
            Field tmp_tmp_field(kTypeChar, compare->child_->next_->val_, expect_val.length(), true);
            tmp_field = &tmp_tmp_field;
          } else {
            //数据类型有误
            return DB_FAILED;
          }
          if (tmp_compare == "=") {
            this_satisfy = real_field->CompareEquals(*tmp_field);
          } else if (tmp_compare == ">") {
            this_satisfy = real_field->CompareGreaterThan(*tmp_field);
          } else if (tmp_compare == "<") {
            this_satisfy = real_field->CompareLessThan(*tmp_field);
          } else if (tmp_compare == ">=") {
            this_satisfy = real_field->CompareGreaterThanEquals(*tmp_field);
          } else if (tmp_compare == "<=") {
            this_satisfy = real_field->CompareLessThanEquals(*tmp_field);
          } else if (tmp_compare == "<>") {
            this_satisfy = real_field->CompareNotEquals(*tmp_field);
          } else {
            //比较符号错误
            return DB_FAILED;
          }
          //第一个条件
          if (first_flag == true) {
            all_satisfy = this_satisfy;
            first_flag = false;
          }
          //后续的累加条件
          else {
            if (tmp_condition == "and") {
              all_satisfy = all_satisfy && this_satisfy;
            } else if (tmp_condition == "or") {
              all_satisfy = all_satisfy || this_satisfy;
            } else {
              //传入了and和or以外的参数
              return DB_FAILED;
            }
            condition = condition->child_;
          }
          compare = compare->next_;
        }
        if (all_satisfy == true) {
          select_rows.push_back(my_row);
        }
      }
    }
    //没有and和or
    else {
      for (auto iter = my_table_info->GetTableHeap()->Begin(nullptr); iter != my_table_info->GetTableHeap()->End();
           iter++) {
        Row my_row = *iter;
        //每一个row是否满足条件
        bool all_satisfy;
        bool this_satisfy;
        std::string tmp_compare = compare->val_;
        string compare_column_name = compare->child_->val_;
        string expect_val = compare->child_->next_->val_;
        TypeId tmp_type;
        uint32_t j;
        for (j = 0; j < my_columns.size(); j++) {
          if (compare_column_name == my_columns[j]->GetName()) {
            tmp_type = my_columns[j]->GetType();
            break;
          }
        }
        Field *real_field = my_row.GetField(j);
        Field *tmp_field;
        if (tmp_type == kTypeInt) {
          int32_t tmp_compare_val = stoi(expect_val);
          Field tmp_tmp_field(kTypeInt, tmp_compare_val);
          tmp_field = &tmp_tmp_field;
        } else if (tmp_type == kTypeFloat) {
          int32_t tmp_compare_val = stof(expect_val);
          Field tmp_tmp_field(kTypeFloat, tmp_compare_val);
          tmp_field = &tmp_tmp_field;
        } else if (tmp_type == kTypeChar) {
          Field tmp_tmp_field(kTypeChar, compare->child_->next_->val_, expect_val.length(), true);
          tmp_field = &tmp_tmp_field;
        } else {
          //数据类型有误
          return DB_FAILED;
        }
        if (tmp_compare == "=") {
          this_satisfy = real_field->CompareEquals(*tmp_field);
        } else if (tmp_compare == ">") {
          this_satisfy = real_field->CompareGreaterThan(*tmp_field);
        } else if (tmp_compare == "<") {
          this_satisfy = real_field->CompareLessThan(*tmp_field);
        } else if (tmp_compare == ">=") {
          this_satisfy = real_field->CompareGreaterThanEquals(*tmp_field);
        } else if (tmp_compare == "<=") {
          this_satisfy = real_field->CompareLessThanEquals(*tmp_field);
        } else if (tmp_compare == "<>") {
          this_satisfy = real_field->CompareNotEquals(*tmp_field);
        } else {
          //比较符号错误
          return DB_FAILED;
        }
        //总共就一个条件
        all_satisfy = this_satisfy;
        if (all_satisfy == true) {
          select_rows.push_back(my_row);
        }
      }
    }
  }
  //测试select结果
  for (uint32_t i = 0; i < select_rows.size(); i++) {
    for (uint32_t j = 0; j < select_rows[i].GetFieldCount(); j++) {
      std::cout << select_rows[i].GetField(j)->GetData() << " ";
    }
    std::cout << endl;
  }
  if (all_columns == true) {
    return DB_SUCCESS;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
  /*#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  return DB_FAILED;*/
  if (current_db_ == "") {
    //未选择database
    return DB_FAILED;
  }
  DBStorageEngine *databese_now = dbs_[current_db_];
  std::string table_name = ast->child_->val_;
  TableInfo *my_table_info = nullptr;
  vector<Column *> my_columns;
  vector<Field> my_fields;
  pSyntaxNode column_value = ast->child_->next_->child_;
  if (databese_now->catalog_mgr_->GetTable(table_name, my_table_info) == DB_FAILED) {
    //找不到该table
    return DB_FAILED;
  } else {
    my_columns = my_table_info->GetSchema()->GetColumns();
  }
  uint32_t i = 0;
  while (i < my_columns.size() && column_value != NULL) {
    TypeId tmp_type = my_columns[i]->GetType();
    if (column_value->type_ == kNodeNull) {
      if (my_columns[i]->IsNullable() == false) {
        //不能为空
        return DB_FAILED;
      } else {
        Field *tmp_field;
        tmp_field->DeserializeFrom(column_value->val_, tmp_type, &tmp_field, true, my_table_info->GetMemHeap());
        my_fields.push_back(*tmp_field);
      }
    } else {
      Field *tmp_field;
      if (tmp_type == kTypeInt) {
        for (uint32_t j = 0; column_value->val_[j] != '\0'; j++) {
          if (column_value->val_[j] < '0' && column_value->val_[j] > '9' && column_value->val_[j] != '-') {
            //不是整数
            return DB_FAILED;
          }
        }
        tmp_field->DeserializeFrom(column_value->val_, tmp_type, &tmp_field, false, my_table_info->GetMemHeap());
      } else if (tmp_type == kTypeFloat) {
        for (uint32_t j = 0; column_value->val_[j] != '\0'; j++) {
          if (column_value->val_[j] < '0' && column_value->val_[j] > '9' && column_value->val_[j] != '-' &&
              column_value->val_[j] != '.') {
            //不是小数
            return DB_FAILED;
          }
        }
        tmp_field->DeserializeFrom(column_value->val_, tmp_type, &tmp_field, false, my_table_info->GetMemHeap());
      } else if (tmp_type == kTypeInvalid) {
        //无效类型
        return DB_FAILED;
      }
      my_fields.push_back(*tmp_field);
    }
  }
  Row my_row(my_fields);
  // 主键/unique判断还没写
  // 插入引起的index更改不会写
  my_table_info->GetTableHeap()->InsertTuple(my_row, nullptr);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
