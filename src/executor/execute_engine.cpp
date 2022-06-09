#include "executor/execute_engine.h"
#include "glog/logging.h"
#include <string>

#include <cstdio>
#include <set>
#include <chrono>
#include <algorithm>
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

#define ENABLE_PARSER_DEBUG

extern "C" {
    int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext* context) {
    if (ast == nullptr) {
        return DB_FAILED;
    }
    dberr_t ret_val = DB_FAILED;
    auto start = std::chrono::high_resolution_clock::now();
    switch (ast->type_) {
    case kNodeCreateDB:
      ret_val = ExecuteCreateDatabase(ast, context);
      break;
    case kNodeDropDB:
        //OK
        ret_val = ExecuteDropDatabase(ast, context);
        break;
    case kNodeShowDB:
        //Not
        ret_val = ExecuteShowDatabases(ast, context);
        break;
    case kNodeUseDB:
        //OK
        ret_val = ExecuteUseDatabase(ast, context);
        break;
    case kNodeShowTables:
        //OK
        ret_val = ExecuteShowTables(ast, context);
        break;
    case kNodeCreateTable:
        //OK
        ret_val = ExecuteCreateTable(ast, context);
        break;
    case kNodeDropTable:
        //OK
        ret_val = ExecuteDropTable(ast, context);
        break;
    case kNodeShowIndexes:
        //OK
        ret_val = ExecuteShowIndexes(ast, context);
        break;
    case kNodeCreateIndex:
        //OK
        ret_val = ExecuteCreateIndex(ast, context);
        break;
    case kNodeDropIndex:
        //?
        ret_val = ExecuteDropIndex(ast, context);
        break;
    case kNodeSelect:
        //?
        ret_val = ExecuteSelect(ast, context);
        break;
    case kNodeInsert:
        //OK
        ret_val = ExecuteInsert(ast, context);
        break;
    case kNodeDelete:
        ret_val = ExecuteDelete(ast, context);
      break;
    case kNodeUpdate:
        ret_val = ExecuteUpdate(ast, context);
      break;
    case kNodeTrxBegin:
        ret_val = ExecuteTrxBegin(ast, context);
      break;
    case kNodeTrxCommit:
        ret_val = ExecuteTrxCommit(ast, context);
      break;
    case kNodeTrxRollback:
        ret_val = ExecuteTrxRollback(ast, context);
      break;
    case kNodeExecFile:
        ret_val = ExecuteExecfile(ast, context);
      break;
    case kNodeQuit:
        ret_val = ExecuteQuit(ast, context);
      break;
    default:
        break;
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<float> duration = end - start;
    std::cout << duration.count() << "s" << std::endl;
    return ret_val;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext* context) {
    std::string database_name = ast->child_->val_;
    //判断是否存在该database文件
    ifstream test_file;
    test_file.open(database_name, ios::in);
    //已存在该文件，但未写入内存
    if (test_file.good()) {
        test_file.close();
        std::cout << "This database has been created already!" << std::endl;
        return DB_FAILED;
    }
    //不存在该文件
    else {
        dbs_[database_name] = new DBStorageEngine(database_name, true);
        ofstream outfile;
        outfile.open("my_databases.txt", ostream::app);
        outfile << database_name << endl;
        outfile.close();
        return DB_SUCCESS;
    }
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext* context) {
    /*#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropIndex" << std::endl;
    #endif
    return DB_FAILED;*/
    std::string database_name = ast->child_->val_;
    //判断是否存在该database文件
    ifstream test_file;
    test_file.open(database_name, ios::in);
    //已存在该文件
    if (test_file.good()) {
        test_file.close();
        if (remove(database_name.c_str()) != 0) {
            std::cout << "Delete failed..." << std::endl;
            return DB_FAILED;
        }
        if (dbs_.find(database_name) != dbs_.end()) {
            auto iter = dbs_.find(database_name);
            dbs_.erase(iter);
        }
        if (database_name == current_db_) {
            current_db_ = "";
        }
        fstream original("my_databases.txt", ios::in);
        fstream safe("tmp.txt", ios::out);
        string tmp_name;
        while (getline(original, tmp_name)) {
            if (tmp_name != database_name) {
                safe << tmp_name << endl;
            }
        }
        original.close();
        safe.close();
        fstream cover("my_databases.txt", ios::out);
        safe.open("tmp.txt", ios::in);
        while (getline(safe, tmp_name)) {
            cover << tmp_name << endl;
        }
        cover.close();
        safe.close();
        remove("tmp.txt");
        return DB_SUCCESS;
    }
    //不存在该文件
    else {
        std::cout << "The database is not exist!" << std::endl;
        return DB_FAILED;
    }
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext* context) {
    /*#ifdef ENABLE_EXECUTE_DEBUG
      LOG(INFO) << "ExecuteDropIndex" << std::endl;
      #endif
      return DB_FAILED;*/
    fstream my_databases("my_databases.txt", ostream::in);
    string database_name;
    while (getline(my_databases, database_name)) {
        cout << database_name << endl;
    }
    my_databases.close();
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext* context) {
    /*#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropIndex" << std::endl;
    #endif
    return DB_FAILED;*/
    std::string database_name = ast->child_->val_;
    //查找该database文件
    auto iter = dbs_.find(database_name);
    if (iter == dbs_.end()) {
        ifstream test_file;
        test_file.open(database_name, ios::in);
        //该database已经存在
        if (test_file.good()) {
            test_file.close();
            dbs_[database_name] = new DBStorageEngine(database_name, false);
            current_db_ = database_name;
            return DB_SUCCESS;
        }
        //不存在该database
        else {
            return DB_FAILED;
        }
    }
    // database已经在内存中
    else {
        current_db_ = iter->first;
        return DB_SUCCESS;
    }
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext* context) {
    /*#ifdef ENABLE_EXECUTE_DEBUG
      LOG(INFO) << "ExecuteDropIndex" << std::endl;
      #endif
      return DB_FAILED;*/

    return DB_SUCCESS;

    if (current_db_ == "") {
        //未选择database
      std::cerr << "no db is chosen\n";
        return DB_FAILED;
    }
    DBStorageEngine* database_now = dbs_[current_db_];
    vector<TableInfo*> my_tables;
    if (database_now->catalog_mgr_->GetTables(my_tables) == DB_FAILED) {
        //无法获取table
        return DB_FAILED;
    }

    for (uint32_t i = 0; i < my_tables.size(); i++) {
        std::cout << my_tables[i]->GetTableId() << " " << my_tables[i]->GetTableName() << endl;
        Schema* my_schema = my_tables[i]->GetSchema();
        vector<Column*> my_columns = my_schema->GetColumns();
        for (uint32_t j = 0; j < my_columns.size(); j++) {
            std::cout << my_columns[j]->GetName() << "(";
            if (my_columns[j]->GetType() == kTypeChar) {
                std::cout << "char"
                    << " " << my_columns[j]->GetLength() << ") ";
            }
            else if (my_columns[j]->GetType() == kTypeInt) {
                std::cout << "int) ";
            }
            else {
                std::cout << "float) ";
            }
        }
        std::cout << endl;
    }

    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext* context) {
  SimpleMemHeap local_heap;

    if (current_db_ == "") {
        //未选择database
      std::cerr << "no db is chosen\n";
        return DB_FAILED;
    }
    DBStorageEngine* database_now = dbs_[current_db_];
    std::string table_name = ast->child_->val_;
    TableInfo* my_tableinfo = nullptr;
    if (database_now->catalog_mgr_->GetTable(table_name, my_tableinfo) == DB_SUCCESS) {
        //该table已存在
        return DB_FAILED;
    }
    pSyntaxNode definition = ast->child_->next_->child_;
    vector<Column*> column_definition;
    vector<std::string> primary_keys;
    vector<Column *> pks;
    uint32_t index = 0;
    //read the schema item
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
        }
        else {
            if (special == "unique") {
                unique = true;
            }
            column_name = definition->child_->val_;
            std::string my_type = definition->child_->next_->val_;
            if (my_type == "int") {
                type = kTypeInt;
            }
            else if (my_type == "float") {
                type = kTypeFloat;
            }
            else if (my_type == "char") {
                type = kTypeChar;
                std::string char_length = definition->child_->next_->child_->val_;
                for (uint32_t i = 0; i < char_length.length(); i++) {
                    if (char_length[i] < '0' || char_length[i] > '9') {
                        //非法输入
                        return DB_FAILED;
                    }
                    length = length * 10 + char_length[i] - '0';
                }
            }
            else {
                type = kTypeInvalid;
            }
            if (type == kTypeChar) {
              Column *tmp = new (database_now->catalog_mgr_->heap_->Allocate(sizeof(Column(column_name, type, length, index, nullable, unique))))
                      Column(column_name, type, length, index, nullable, unique);
                column_definition.push_back(tmp);
            }
            else {
              Column *tmp = new (
                  database_now->catalog_mgr_->heap_->Allocate(sizeof(Column(column_name, type, index, nullable, unique))))
                      Column(column_name, type, index, nullable, unique);
                column_definition.push_back(tmp);
            }
        }
        definition = definition->next_;
        index++;
    }
    //transfer the string pk into Column* pk
    if (TransferPks(primary_keys, column_definition, pks) == DB_FAILED) {
      std::cerr << "Primary Key Definite Failed\n";
      return DB_FAILED;
    }

    
    TableSchema *my_schema = new TableSchema(column_definition, pks);
    // call the create table
    return database_now->catalog_mgr_->CreateTable(table_name, my_schema, nullptr, my_tableinfo);
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext* context) {
    /*#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropIndex" << std::endl;
    #endif
    return DB_FAILED;*/
    if (current_db_ == "") {
        //未选择database
      std::cerr << "no db is chosen\n";
        return DB_FAILED;
    }
    std::string table_name = ast->child_->val_;
    DBStorageEngine* database_now = dbs_[current_db_];
    return database_now->catalog_mgr_->DropTable(table_name);
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext* context) {
    /*#ifdef ENABLE_EXECUTE_DEBUG
      LOG(INFO) << "ExecuteDropIndex" << std::endl;
      #endif
      return DB_FAILED;*/
    if (current_db_ == "") {
        //未选择database
      std::cerr << "no db is chosen\n";
        return DB_FAILED;
    }
    DBStorageEngine* database_now = dbs_[current_db_];
    
    for (auto i = database_now->catalog_mgr_->GetIndexs_()->begin();
         i != database_now->catalog_mgr_->GetIndexs_()->end(); i++) {
        //table name and index name
      cout << i->second->GetTableInfo()->GetTableName() << "\t"
           << i->second->GetIndexName() << "\t";
      //index keys
      for (auto j = i->second->GetIndexKeySchema()->GetColumns().begin();
           j != i->second->GetIndexKeySchema()->GetColumns().end(); j++) {
        cout << (*j)->GetName() << ",";
      }
      cout << std::endl;
    }
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext* context) {
    /*#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropIndex" << std::endl;
    #endif
    return DB_FAILED;*/
    if (current_db_ == "") {
        //未选择database
      std::cerr << "no db is chosen\n";
        return DB_FAILED;
    }
    DBStorageEngine* database_now = dbs_[current_db_];
    std::string index_name = ast->child_->val_;
    std::string table_name = ast->child_->next_->val_;
    TableInfo* my_table_info = nullptr;
    IndexInfo* my_index_info = nullptr;
    if (database_now->catalog_mgr_->GetTable(table_name, my_table_info) == DB_FAILED) {
        //该table不存在
        return DB_FAILED;
    }
    if (database_now->catalog_mgr_->GetIndex(table_name, index_name, my_index_info) == DB_SUCCESS) {
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
    IndexInfo* index_info = nullptr;
    return database_now->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, nullptr, index_info);
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext* context) {
    /*#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropIndex" << std::endl;
    #endif
    return DB_FAILED;*/
    if (current_db_ == "") {
        //未选择database
      std::cerr << "no db is chosen\n";
        return DB_FAILED;
    }
    DBStorageEngine* database_now = dbs_[current_db_];
    std::string index_name = ast->child_->val_;
    auto i = database_now->catalog_mgr_->GetIndexNames_()->begin();
    for (; i != database_now->catalog_mgr_->GetIndexNames_()->end(); i++) {
      if (i->second.find(index_name) != i->second.end()) break;
    }
    if (i == database_now->catalog_mgr_->GetIndexNames_()->end())
      return DB_INDEX_NOT_FOUND;
    else {
      return database_now->catalog_mgr_->DropIndex(i->first, index_name);
    }
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext* context) {
    /*#ifdef ENABLE_EXECUTE_DEBUG
      LOG(INFO) << "ExecuteDropIndex" << std::endl;
      #endif
      return DB_FAILED;*/
  SimpleMemHeap local_heap;

    if (current_db_ == "") {
        //未选择database
      std::cerr << "no db is chosen\n";
        return DB_FAILED;
    }
    DBStorageEngine* database_now = dbs_[current_db_];
    //符合条件的row
    std::vector<Row> select_rows;
    pSyntaxNode select_type = ast->child_;
    bool all_columns = false;
    std::vector<string> select_column_name;
    std::set<uint32_t> column_map;
    // select * 模式
    if (select_type->type_ == kNodeAllColumns) {
        all_columns = true;
    }
    // select xxx, xxx, ... 模式
    else {
        pSyntaxNode select_column_node = select_type->child_;
        while (select_column_node != NULL) {
            string tmp_column_name = select_column_node->val_;
            select_column_name.push_back(tmp_column_name);
            select_column_node = select_column_node->next_;
        }
    }
    // from xxx
    // only one table is supported
    std::string table_name = select_type->next_->val_;
    TableInfo* my_table_info = nullptr;
    if (database_now->catalog_mgr_->GetTable(table_name, my_table_info)) {
        //找不到该table
      std::cerr << "No such table\n";
        return DB_FAILED;
    }
    vector<Column*> my_columns = my_table_info->GetSchema()->GetColumns();
    for (auto i : select_column_name) {
      uint32_t index = 0;
      if (my_table_info->GetSchema()->GetColumnIndex(i, index)) {
        std::cerr << "Wrong column\n";
        return DB_FAILED;
      }
      column_map.insert(index);
    }

    // from xxx后面没有where
    if (select_type->next_->next_ == NULL) {
        for (auto iter = my_table_info->GetTableHeap()->Begin(nullptr); iter != my_table_info->GetTableHeap()->End();
            iter++) {
            Row my_row = *iter;
            select_rows.push_back(my_row);
        }
    }
    // from xxx后面有where
    else {
        pSyntaxNode condition = select_type->next_->next_->child_;
        pSyntaxNode compare = condition;
        std::vector<pSyntaxNode> conditions;
        std::string oper, col_name, val;

        //get the leftest node
        std::string tmp_val = compare->val_;
        while (tmp_val == "and" || tmp_val == "or") {
          conditions.push_back(compare);
          compare = compare->child_;
          tmp_val = compare->val_;
        }
        
        std::vector<Row> base_set;
        std::vector<Row> another_set;
        //1. get the base
        oper = compare->val_;
        col_name = compare->child_->val_;
        val = compare->child_->next_->val_;
        GetRowSet(database_now, base_set, my_table_info, oper, col_name, val, local_heap);
        select_rows = base_set;
        //2. has at least one condition
        while (conditions.empty() == false) {
          select_rows.clear();
          condition = conditions.back();
          conditions.pop_back();
          compare = condition->child_->next_;
          oper = compare->val_;
          col_name = compare->child_->val_;
          val = compare->child_->next_->val_;
          GetRowSet(database_now, another_set, my_table_info, oper, col_name, val, local_heap);
          //union / intersect
          tmp_val = condition->val_;
          if (tmp_val == "and")
            Intersect(base_set, another_set, select_rows);
          else if (tmp_val == "or")
            Union(base_set, another_set, select_rows);

          base_set = select_rows;
        }
        
    }

    if (all_columns == true) {
      for (auto i : select_rows) {
        for (auto j : i.GetFields()) {
          std::cout << j->GetData() << "\t";
        }
        std::cout << std::endl;
      }

        return DB_SUCCESS;
    }

    for (auto i : select_rows) {
      int count = 0;
      for (auto j : i.GetFields()) {
        if (column_map.find(count)!=column_map.end())
            std::cout << j->GetData() << "\t";

        count++;
      }
      std::cout << std::endl;
    }
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext* context) {
    /*#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteInsert" << std::endl;
    #endif
    return DB_FAILED;*/
    if (current_db_ == "") {
        //未选择database
        std::cerr << "no db is chosen\n";
        return DB_FAILED;
    }
    DBStorageEngine* database_now = dbs_[current_db_];
    std::string table_name = ast->child_->val_;
    TableInfo* my_table_info = nullptr;
    vector<Column*> my_columns;
    vector<Field> my_fields;
    vector<Field> pk_fields;
    pSyntaxNode column_value = ast->child_->next_->child_;
    if (database_now->catalog_mgr_->GetTable(table_name, my_table_info)) {
        //找不到该table
        return DB_FAILED;
    }
    else {
        my_columns = my_table_info->GetSchema()->GetColumns();
    }
    uint32_t i = 0;
    while (i < my_columns.size() && column_value != NULL) {
        TypeId tmp_type = my_columns[i]->GetType();
        if (column_value->type_ == kNodeNull) {
            if (my_columns[i]->IsNullable() == false) {
                //不能为空
                return DB_FAILED;
            }
            else {
              my_fields.push_back(Field(my_columns[i]->GetType()));
            }
        }
        else {
            if (tmp_type == kTypeInt) {
                string tmp_val = column_value->val_;
                for (uint32_t j = 0; j < tmp_val.length(); j++) {
                    if (tmp_val[j] < '0' && tmp_val[j] > '9' && tmp_val[j] != '-') {
                        //不是整数
                        return DB_FAILED;
                    }
                }
                my_fields.push_back(Field(kTypeInt, stoi(column_value->val_)));
            }
            else if (tmp_type == kTypeFloat) {
                string tmp_val = column_value->val_;
                for (uint32_t j = 0; j < tmp_val.length(); j++) {
                    if (tmp_val[j] < '0' && tmp_val[j] > '9' && tmp_val[j] != '-' && tmp_val[j] != '.') {
                        //不是小数
                        return DB_FAILED;
                    }
                }
                float tmp_float = stof(column_value->val_);
                my_fields.push_back(Field(kTypeFloat, tmp_float));
            }
            else if (tmp_type == kTypeInvalid) {
                //无效类型
                return DB_FAILED;
            }
            else if (tmp_type == kTypeChar) {
                bool manage;
              char *tmp_val = column_value->val_;
                if (strlen(tmp_val) < my_columns[i]->GetLength()) {
                    manage = false;
                }
                else {
                    manage = true;
                }
                my_fields.push_back(Field(kTypeChar, tmp_val, strlen(tmp_val), manage));
            }
        }
        i++;
        column_value = column_value->next_;
    }
    if (column_value != NULL) {
        std::cout << "Too many elements!" << std::endl;
    }
    Row *my_row = new Row(my_fields);
    dberr_t ret_val = Insert(database_now, my_table_info, my_row);
    delete my_row;

    return ret_val;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
    SimpleMemHeap local_heap;
    if (current_db_ == "") {
      //未选择database
      std::cerr << "no db is chosen\n";
      return DB_FAILED;
    }
    DBStorageEngine *database_now = dbs_[current_db_];
    //符合条件的row
    std::vector<Row> select_rows;

    // from xxx
    // only one table is supported
    std::string table_name = ast->child_->val_;
    TableInfo *my_table_info = nullptr;
    if (database_now->catalog_mgr_->GetTable(table_name, my_table_info) == DB_FAILED) {
      //找不到该table
      return DB_FAILED;
    }

    // from xxx后面没有where
    if (ast->child_->next_ == NULL) {
      for (auto iter = my_table_info->GetTableHeap()->Begin(nullptr); iter != my_table_info->GetTableHeap()->End();
           iter++) {
        Row my_row = *iter;
        select_rows.push_back(my_row);
      }
    }
    // from xxx后面有where
    else {
      pSyntaxNode condition = ast->child_->next_->child_;
      pSyntaxNode compare = condition;
      std::vector<pSyntaxNode> conditions;
      std::string oper, col_name, val;

      // get the leftest node
      std::string tmp_val = compare->val_;
      while (tmp_val == "and" || tmp_val == "or") {
        conditions.push_back(compare);
        compare = compare->child_;
        tmp_val = compare->val_;
      }

      std::vector<Row> base_set;
      std::vector<Row> another_set;
      // 1. get the base
      oper = compare->val_;
      col_name = compare->child_->val_;
      val = compare->child_->next_->val_;
      GetRowSet(database_now, base_set, my_table_info, oper, col_name, val, local_heap);
      select_rows = base_set;
      // 2. has at least one condition
      while (conditions.empty() == false) {
        select_rows.clear();
        condition = conditions.back();
        conditions.pop_back();
        compare = condition->child_->next_;
        oper = compare->val_;
        col_name = compare->child_->val_;
        val = compare->child_->next_->val_;
        GetRowSet(database_now, another_set, my_table_info, oper, col_name, val, local_heap);
        // union / intersect
        tmp_val = condition->val_;
        if (tmp_val == "and")
          Intersect(base_set, another_set, select_rows);
        else if (tmp_val == "or")
          Union(base_set, another_set, select_rows);

        base_set = select_rows;
      }
    }

    std::cout << select_rows.size() << " rows are effected\n";
    IndexInfo *tmp_index;
    for (auto z = select_rows.begin(); z != select_rows.end(); z++) {
      // delete from table
      my_table_info->GetTableHeap()->MarkDelete(z->GetRowId(), nullptr);
      // delete from index
      for (auto i = ((*(database_now->catalog_mgr_->GetIndexNames_()))[table_name]).begin();
           i != ((*(database_now->catalog_mgr_->GetIndexNames_()))[table_name]).end(); i++) {
        std::vector<Field> key_field;
        tmp_index = (*(database_now->catalog_mgr_->GetIndexs_()))[i->second];
        // get keys
        for (auto j = tmp_index->GetIndexKeySchema()->GetColumns().begin();
             j != tmp_index->GetIndexKeySchema()->GetColumns().end(); j++)
          key_field.push_back(*(z->GetField((*j)->GetTableInd())));
        Row key = Row(key_field);
        (*(database_now->catalog_mgr_->GetIndexs_()))[i->second]->GetIndex()->RemoveEntry(key, z->GetRowId(), nullptr);
      }
    }
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
    SimpleMemHeap local_heap;
    if (current_db_ == "") {
      //未选择database
      std::cerr << "no db is chosen\n";
      return DB_FAILED;
    }
    DBStorageEngine *database_now = dbs_[current_db_];
    //符合条件的row
    std::vector<Row> select_rows;

    // from xxx
    // only one table is supported
    std::string table_name = ast->child_->val_;
    TableInfo *my_table_info = nullptr;
    if (database_now->catalog_mgr_->GetTable(table_name, my_table_info) == DB_FAILED) {
      //找不到该table
      return DB_FAILED;
    }

    // get the new value
    pSyntaxNode update_val = ast->child_->next_->child_;
    std::vector<uint32_t> key_map;
    std::vector<std::string> new_val;
    std::string tmp_str;
    while (update_val != nullptr) {
      uint32_t index = 0;
      if (my_table_info->GetSchema()->GetColumnIndex(update_val->child_->val_, index)) {
        std::cerr << "No such column\n";
        return DB_FAILED;
      }
      tmp_str = update_val->child_->next_->val_;
      key_map.push_back(index);
      new_val.push_back(tmp_str);

      update_val = update_val->next_;
    }

    // from xxx后面没有where
    if (ast->child_->next_->next_ == NULL) {
      for (auto iter = my_table_info->GetTableHeap()->Begin(nullptr); iter != my_table_info->GetTableHeap()->End();
           iter++) {
        Row my_row = *iter;
        select_rows.push_back(my_row);
      }
    }
    // from xxx后面有where
    else {
      pSyntaxNode condition = ast->child_->next_->next_->child_;
      pSyntaxNode compare = condition;
      std::vector<pSyntaxNode> conditions;
      std::string oper, col_name, val;

      // get the leftest node
      std::string tmp_val = compare->val_;
      while (tmp_val == "and" || tmp_val == "or") {
        conditions.push_back(compare);
        compare = compare->child_;
        tmp_val = compare->val_;
      }

      std::vector<Row> base_set;
      std::vector<Row> another_set;
      // 1. get the base
      oper = compare->val_;
      col_name = compare->child_->val_;
      val = compare->child_->next_->val_;
      GetRowSet(database_now, base_set, my_table_info, oper, col_name, val, local_heap);
      select_rows = base_set;
      // 2. has at least one condition
      while (conditions.empty() == false) {
        select_rows.clear();
        condition = conditions.back();
        conditions.pop_back();
        compare = condition->child_->next_;
        oper = compare->val_;
        col_name = compare->child_->val_;
        val = compare->child_->next_->val_;
        GetRowSet(database_now, another_set, my_table_info, oper, col_name, val, local_heap);
        // union / intersect
        tmp_val = condition->val_;
        if (tmp_val == "and")
          Intersect(base_set, another_set, select_rows);
        else if (tmp_val == "or")
          Union(base_set, another_set, select_rows);

        base_set = select_rows;
      }
    }

    std::cout << select_rows.size() << " rows are effected\n";
    //delete
    IndexInfo *tmp_index;
    for (auto z = select_rows.begin(); z != select_rows.end(); z++) {
      // delete from table
      my_table_info->GetTableHeap()->MarkDelete(z->GetRowId(), nullptr);
      // delete from index
      for (auto i = ((*(database_now->catalog_mgr_->GetIndexNames_()))[table_name]).begin();
           i != ((*(database_now->catalog_mgr_->GetIndexNames_()))[table_name]).end(); i++) {
        std::vector<Field> key_field;
        tmp_index = (*(database_now->catalog_mgr_->GetIndexs_()))[i->second];
        // get keys
        for (auto j = tmp_index->GetIndexKeySchema()->GetColumns().begin();
             j != tmp_index->GetIndexKeySchema()->GetColumns().end(); j++)
          key_field.push_back(*(z->GetField((*j)->GetTableInd())));
        Row key = Row(key_field);
        (*(database_now->catalog_mgr_->GetIndexs_()))[i->second]->GetIndex()->RemoveEntry(key, z->GetRowId(), nullptr);
      }
    }
    // prepare to insert
    for (auto z = select_rows.begin(); z != select_rows.end(); z++) {
      for (auto j = key_map.begin(); j != key_map.end();j++) {
        z->GetFields()[*j] =
            MakeField(new_val[j - key_map.begin()], my_table_info->GetSchema()->GetColumn(*j)->GetType(), local_heap);
      }
      // insert
      Insert(database_now, my_table_info, &(*z));
    }

    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext* context) {
    /*#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteInsert" << std::endl;
    #endif
    return DB_FAILED;*/
    std::string txt_name = ast->child_->val_;
    ifstream test_file;
    test_file.open(txt_name, ios::in);
    //该文件存在
    if (test_file.good()) {
        test_file.close();
        FILE* txt = fopen(txt_name.c_str(), "r");
        char code[1024];
        while (!feof(txt)) {
            fscanf(txt, "%[^;]%*c", code);
            if (feof(txt)) break;
            strcat(code, ";");
            YY_BUFFER_STATE bp = yy_scan_string(code);
            cout << "code = " << code << endl;
            if (bp == nullptr) {
                LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
                exit(-1);
            }
            yy_switch_to_buffer(bp);
            MinisqlParserInit();
            yyparse();
            if (MinisqlParserGetError()) {
                printf("%s\n", MinisqlParserGetErrorMessage());
            }
            Execute(MinisqlGetParserRootNode(), context);
            //sleep(1);
            MinisqlParserFinish();
            yy_delete_buffer(bp);
            yylex_destroy();
            if (context->flag_quit_) {
                printf("bye!\n");
                break;
            }
        }
        fclose(txt);
        return DB_SUCCESS;
    }
    //该文件不存在
    else {
        cout << "The file is not exist!" << endl;
        return DB_SUCCESS;
    }
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
    ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
    context->flag_quit_ = true;
    return DB_SUCCESS;
}

//Support function
//create table
dberr_t ExecuteEngine::TransferPks(std::vector<std::string> &in, std::vector<Column *> item,
                                   std::vector<Column *> &out) {
  auto i = in.begin();
  auto j = item.begin();
  for (i = in.begin(); i != in.end();i++) {
    for (j = item.begin(); j != item.end();j++) {
      if ((*j)->GetName() == *i) break;
    }
    if (j == item.end())
      return DB_FAILED;
    else
      out.push_back(*j);
  }

  return DB_SUCCESS;
}
//Where clouse
Field *ExecuteEngine::MakeField(std::string &expect_val, TypeId tmp_type,SimpleMemHeap &heap) {  
  Field *tmp_field;
  if (tmp_type == kTypeInt) {
    int32_t tmp_compare_val = stoi(expect_val);
    tmp_field = new (heap.Allocate(sizeof(Field(kTypeInt, tmp_compare_val)))) Field(kTypeInt, tmp_compare_val);
  } else if (tmp_type == kTypeFloat) {
    float tmp_compare_val = stof(expect_val);
    tmp_field = new (heap.Allocate(sizeof(Field(kTypeFloat, tmp_compare_val)))) Field(kTypeFloat, tmp_compare_val);
  } else if (tmp_type == kTypeChar) {
    tmp_field = new (
        heap.Allocate(sizeof(Field(kTypeChar, const_cast<char *>(expect_val.c_str()), expect_val.length(), false))))
        Field(kTypeChar, const_cast<char *>(expect_val.c_str()), expect_val.length(), false);
  } else {
    //数据类型有误
    return nullptr;
  }

  return tmp_field;
}

dberr_t ExecuteEngine::GetRowSet(DBStorageEngine *database_now, std::vector<Row> &result, TableInfo *table,
                                 std::string &condition,
               string compare_column_name,std::string &val,SimpleMemHeap &heap) {
  //get the column
  Column *target = nullptr;
  for (auto i = table->GetSchema()->GetColumns().begin(); i != table->GetSchema()->GetColumns().end();i++) {
    if ((*i)->GetName() == compare_column_name) {
      target = (*i);
      break;
    }
    if (i == table->GetSchema()->GetColumns().end()) {
      std::cerr << "Where condition is wrong, Cannot find the column\n";
      return DB_FAILED;
    }
  }
  //try to find a index
  auto vec = &((*(database_now->catalog_mgr_->GetIndexNames_()))[table->GetTableName()]);
  auto i = vec->begin();
  for (; i != vec->end(); i++) {
    uint32_t num = 0;
    if (((*(database_now->catalog_mgr_->GetIndexs_()))[i->second])->GetIndexKeySchema()->GetColumnCount() == 1 &&
        ((*(database_now->catalog_mgr_->GetIndexs_()))[i->second])
                ->GetIndexKeySchema()
                ->GetColumnIndex(compare_column_name, num) == DB_SUCCESS) {
      break;
    }
  }
  //get the key
  Field *key_f = MakeField(val, target->GetType(), heap);
  Field *record_field = nullptr;
  if (key_f == nullptr) {
    std::cerr << "Field make failed\n";
    return DB_FAILED;
  }

  std::vector<Field> key_fs;
  key_fs.push_back(*key_f);
  Row key = Row(key_fs);
  std::vector<RowId> result_ids;
  
  if (condition == "=") {
    if (i != vec->end()) {
      ((*(database_now->catalog_mgr_->GetIndexs_()))[i->second])->GetIndex()->ScanKey(key, result_ids, nullptr);
      for (auto j = result_ids.begin(); j != result_ids.end(); j++)
      {
        Row tmp_row = Row(*j);
        table->GetTableHeap()->GetTuple(&tmp_row, nullptr);
        result.push_back(tmp_row);
      }
    }else {
      for (auto i = table->GetTableHeap()->Begin(nullptr); i != table->GetTableHeap()->End(); i++) {
        record_field = i->GetField(target->GetTableInd());
        if (record_field->CompareEquals(*key_f) == true) result.push_back(*i);
      }
    }



  } else if (condition == ">") {
    for (auto i = table->GetTableHeap()->Begin(nullptr); i != table->GetTableHeap()->End(); i++) {
      record_field = i->GetField(target->GetTableInd());
      if (record_field->CompareGreaterThan(*key_f) == true) result.push_back(*i);
    }
  } else if (condition == "<") {
    for (auto i = table->GetTableHeap()->Begin(nullptr); i != table->GetTableHeap()->End(); i++) {
      record_field = i->GetField(target->GetTableInd());
      if (record_field->CompareLessThan(*key_f) == true) result.push_back(*i);
    }
  } else if (condition == ">=") {
    for (auto i = table->GetTableHeap()->Begin(nullptr); i != table->GetTableHeap()->End(); i++) {
      record_field = i->GetField(target->GetTableInd());
      if (record_field->CompareGreaterThanEquals(*key_f) == true) result.push_back(*i);
    }
  } else if (condition == "<=") {
    for (auto i = table->GetTableHeap()->Begin(nullptr); i != table->GetTableHeap()->End(); i++) {
      record_field = i->GetField(target->GetTableInd());
      if (record_field->CompareLessThanEquals(*key_f) == true) result.push_back(*i);
    }
  } else if (condition == "<>") {
    for (auto i = table->GetTableHeap()->Begin(nullptr); i != table->GetTableHeap()->End(); i++) {
      record_field = i->GetField(target->GetTableInd());
      if (record_field->CompareNotEquals(*key_f) == true) result.push_back(*i);
    }
  }

  return DB_SUCCESS;
}

void ExecuteEngine::Intersect(std::vector<Row> &a, std::vector<Row> &b, std::vector<Row> &result) {
  std::unordered_map<int64_t, Row *> map;
  for (auto i = a.begin(); i != a.end(); i++) {
    map.insert(make_pair((i->GetRowId().Get()), &(*i)));
  }

  for (auto i = b.begin(); i != b.end(); i++) {
    if (map.find(i->GetRowId().Get()) != map.end()) {
      result.push_back(*i);
    } else {
      // not exists in both set
    }
  }
}

void ExecuteEngine::Union(std::vector<Row> &a, std::vector<Row> &b, std::vector<Row> &result) {
  std::unordered_map<int64_t, Row *> map;
  for (auto i = a.begin(); i != a.end(); i++) {
    map.insert(make_pair((i->GetRowId().Get()), &(*i)));
    result.push_back(*i);
  }

  for (auto i = b.begin(); i != b.end(); i++) {
    if (map.find(i->GetRowId().Get()) != map.end()) {
        //already exists
    } else {
      result.push_back(*i);
    }
  }
}

dberr_t ExecuteEngine::Insert(DBStorageEngine *database_now, TableInfo *my_table_info, Row *my_row) {
  // get pks
  vector<Field> pk_fields;
  for (auto i : my_table_info->GetSchema()->GetPks()) {
    if (my_row->GetField(i->GetTableInd())->IsNull() == true) {
      std::cerr << "PK IS NULL\n";
      return DB_FAILED;
    }
    pk_fields.push_back(*(my_row->GetField(i->GetTableInd())));
  }
  Row pk = Row(pk_fields);
  // 主键/unique判断
  // 1. check the pk
  if (((*(database_now->catalog_mgr_->GetIndexNames_()))[my_table_info->GetTableName()]).find(PKINDEX) ==
      ((*(database_now->catalog_mgr_->GetIndexNames_()))[my_table_info->GetTableName()]).end()) {
    std::cerr << "PK INDEX CANNT FIND\n";
    return DB_FAILED;
  }

  index_id_t tmp_index_id = ((*(database_now->catalog_mgr_->GetIndexNames_()))[my_table_info->GetTableName()])[PKINDEX];
  if (database_now->catalog_mgr_->GetIndexs_()->find(tmp_index_id) == database_now->catalog_mgr_->GetIndexs_()->end()) {
    std::cerr << "PK INDEX CANNT FIND\n";
    return DB_FAILED;
  }

  IndexInfo *tmp_index = (*(database_now->catalog_mgr_->GetIndexs_()))[tmp_index_id];
  std::vector<RowId> tmp_result;
  tmp_index->GetIndex()->ScanKey(pk, tmp_result, nullptr);
  if (tmp_result.empty() != true) {
    std::cerr << "PK Already exists\n";
    return DB_FAILED;
  }

  // 2. check the unique
  for (auto i = my_table_info->GetSchema()->GetColumns().begin(); i != my_table_info->GetSchema()->GetColumns().end();
       i++) {
    int count = 1;
    if ((*i)->IsUnique() == true) {
      tmp_index_id = ((*(database_now->catalog_mgr_
                             ->GetIndexNames_()))[my_table_info->GetTableName()])[UNIQUE_INDEX + std::to_string(count)];
      tmp_index = (*(database_now->catalog_mgr_->GetIndexs_()))[tmp_index_id];
      tmp_result.clear();
      pk_fields.clear();
      pk_fields.push_back(*(my_row->GetField((*i)->GetTableInd())));
      Row key = Row(pk_fields);
      tmp_index->GetIndex()->ScanKey(key, tmp_result, nullptr);

      if (tmp_result.empty() != true) {
        std::cerr << "Unique item " << (*i)->GetName() << " Already exists\n ";
        return DB_FAILED;
      }

      count++;
    }
  }

  // insert into table
  my_table_info->GetTableHeap()->InsertTuple(*my_row, nullptr);
  // insert into index
  for (auto i = ((*(database_now->catalog_mgr_->GetIndexNames_()))[my_table_info->GetTableName()]).begin();
       i != ((*(database_now->catalog_mgr_->GetIndexNames_()))[my_table_info->GetTableName()]).end(); i++) {
    std::vector<Field> key_field;
    tmp_index = (*(database_now->catalog_mgr_->GetIndexs_()))[i->second];
    // get keys
    for (auto j = tmp_index->GetIndexKeySchema()->GetColumns().begin();
         j != tmp_index->GetIndexKeySchema()->GetColumns().end(); j++)
      key_field.push_back(*(my_row->GetField((*j)->GetTableInd())));
    Row key = Row(key_field);
    (*(database_now->catalog_mgr_->GetIndexs_()))[i->second]->GetIndex()->InsertEntry(key, my_row->GetRowId(), nullptr);
  }

  return DB_SUCCESS;
}