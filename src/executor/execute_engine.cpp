#include "executor/execute_engine.h"
#include "glog/logging.h"
#include <string>

#include <cstdio>
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
    if (current_db_ == "") {
        //未选择database
        return DB_FAILED;
    }
    DBStorageEngine* database_now = dbs_[current_db_];
    vector<TableInfo*> my_tables;
    if (database_now->catalog_mgr_->GetTables(my_tables) == DB_FAILED) {
        //无法获取table
        return DB_FAILED;
    }
    ofstream outfile;
    outfile.open("show_tables.txt", ostream::out);
    for (uint32_t i = 0; i < my_tables.size(); i++) {
        outfile << my_tables[i]->GetTableId() << " " << my_tables[i]->GetTableName() << endl;
        Schema* my_schema = my_tables[i]->GetSchema();
        vector<Column*> my_columns = my_schema->GetColumns();
        for (uint32_t j = 0; j < my_columns.size(); j++) {
            outfile << my_columns[j]->GetName() << "(";
            if (my_columns[j]->GetType() == kTypeChar) {
                outfile << "char"
                    << " " << my_columns[j]->GetLength() << ") ";
            }
            else if (my_columns[j]->GetType() == kTypeInt) {
                outfile << "int) ";
            }
            else {
                outfile << "float) ";
            }
        }
        outfile << endl;
    }
    outfile.close();
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext* context) {

    if (current_db_ == "") {
        //未选择database
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
                Column* tmp = new Column(column_name, type, length, index, nullable, unique);
                column_definition.push_back(tmp);
            }
            else {
                Column* tmp = new Column(column_name, type, index, nullable, unique);
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
        return DB_FAILED;
    }
    DBStorageEngine* database_now = dbs_[current_db_];
    std::string index_name = ast->child_->val_;
    std::string table_name = ast->child_->val_;
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
    if (current_db_ == "") {
        //未选择database
        return DB_FAILED;
    }
    DBStorageEngine* database_now = dbs_[current_db_];
    //符合条件的row
    vector<Row> select_rows;
    pSyntaxNode select_type = ast->child_;
    bool all_columns = false;
    vector<string> select_column_name;
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
    // select ... from xxx
    std::string table_name = select_type->next_->val_;
    TableInfo* my_table_info = nullptr;
    if (database_now->catalog_mgr_->GetTable(table_name, my_table_info) == DB_FAILED) {
        //找不到该table
        return DB_FAILED;
    }
    vector<Column*> my_columns = my_table_info->GetSchema()->GetColumns();
    // select ... from xxx后面没有where
    if (select_type->next_->next_ == NULL) {
        for (auto iter = my_table_info->GetTableHeap()->Begin(nullptr); iter != my_table_info->GetTableHeap()->End();
            iter++) {
            Row my_row = *iter;
            select_rows.push_back(my_row);
        }
    }
    // select * from xxx后面有where
    else {
        pSyntaxNode condition = select_type->next_->next_->child_;
        std::string what_char = condition->val_;
        pSyntaxNode compare = condition;
        vector<pSyntaxNode> my_conditions;
        //有and和or
        if (what_char == "and" || what_char == "or") {
            while (1) {
                std::string tmp_val = compare->val_;
                if (tmp_val != "and" && tmp_val != "or") {
                    break;
                }
                my_conditions.push_back(compare);
                compare = compare->child_;
            }
            //compare一开始是最左下方的一个判断条件，节点在"="这种符号上
            for (auto iter = my_table_info->GetTableHeap()->Begin(nullptr); iter != my_table_info->GetTableHeap()->End();
                iter++) {
                int now = my_conditions.size() - 1;
                Row my_row = *iter;
                //每一个row是否满足条件
                bool first_flag = true;
                bool all_satisfy;
                bool this_satisfy;
                while (1) {
                    std::string tmp_condition = my_conditions[now]->val_;
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
                    Field* real_field = my_row.GetField(j);
                    Field* tmp_field;
                    if (tmp_type == kTypeInt) {
                        int32_t tmp_compare_val = stoi(expect_val);
                        tmp_field =  new Field(kTypeInt, tmp_compare_val);
                    }
                    else if (tmp_type == kTypeFloat) {
                        int32_t tmp_compare_val = stof(expect_val);
                        tmp_field = new Field(kTypeFloat, tmp_compare_val);
                    }
                    else if (tmp_type == kTypeChar) {
                      char *tmp_compare_val = compare->child_->next_->val_;
                      /*if (tmp_compare_val[strlen(tmp_compare_val) - 1] == '\b') {
                        tmp_compare_val[strlen(tmp_compare_val) - 1] = '\0';
                      }*/
                      tmp_field = new Field(kTypeChar, tmp_compare_val, strlen(tmp_compare_val), false);
                    }
                    else {
                        //数据类型有误
                        return DB_FAILED;
                    }
                    if (tmp_compare == "=") {
                        this_satisfy = real_field->CompareEquals(*tmp_field);
                    }
                    else if (tmp_compare == ">") {
                        this_satisfy = real_field->CompareGreaterThan(*tmp_field);
                    }
                    else if (tmp_compare == "<") {
                        this_satisfy = real_field->CompareLessThan(*tmp_field);
                    }
                    else if (tmp_compare == ">=") {
                        this_satisfy = real_field->CompareGreaterThanEquals(*tmp_field);
                    }
                    else if (tmp_compare == "<=") {
                        this_satisfy = real_field->CompareLessThanEquals(*tmp_field);
                    }
                    else if (tmp_compare == "<>") {
                        this_satisfy = real_field->CompareNotEquals(*tmp_field);
                    }
                    else {
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
                        }
                        else if (tmp_condition == "or") {
                            all_satisfy = all_satisfy || this_satisfy;
                        }
                        else {
                            //传入了and和or以外的参数
                            return DB_FAILED;
                        }
                        now--;
                        if (now < 0) {
                          break;
                        }
                    }
                    compare = my_conditions[now]->child_->next_;
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
                Field* real_field = my_row.GetField(j);
                Field* tmp_field;
                if (tmp_type == kTypeInt) {
                  int32_t tmp_compare_val = stoi(expect_val);
                  tmp_field = new Field(kTypeInt, tmp_compare_val);
                }
                else if (tmp_type == kTypeFloat) {
                  int32_t tmp_compare_val = stof(expect_val);
                  tmp_field = new Field(kTypeFloat, tmp_compare_val);
                }
                else if (tmp_type == kTypeChar) {
                  tmp_field = new Field(kTypeChar, compare->child_->next_->val_, expect_val.length(), false);
                }
                else {
                    //数据类型有误
                    return DB_FAILED;
                }
                if (tmp_compare == "=") {
                    this_satisfy = real_field->CompareEquals(*tmp_field);
                }
                else if (tmp_compare == ">") {
                    this_satisfy = real_field->CompareGreaterThan(*tmp_field);
                }
                else if (tmp_compare == "<") {
                    this_satisfy = real_field->CompareLessThan(*tmp_field);
                }
                else if (tmp_compare == ">=") {
                    this_satisfy = real_field->CompareGreaterThanEquals(*tmp_field);
                }
                else if (tmp_compare == "<=") {
                    this_satisfy = real_field->CompareLessThanEquals(*tmp_field);
                }
                else if (tmp_compare == "<>") {
                    this_satisfy = real_field->CompareNotEquals(*tmp_field);
                }
                else {
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
    if (all_columns == true) {
        return DB_SUCCESS;
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
        return DB_FAILED;
    }
    DBStorageEngine* database_now = dbs_[current_db_];
    std::string table_name = ast->child_->val_;
    TableInfo* my_table_info = nullptr;
    vector<Column*> my_columns;
    vector<Field> my_fields;
    pSyntaxNode column_value = ast->child_->next_->child_;
    if (database_now->catalog_mgr_->GetTable(table_name, my_table_info) == DB_FAILED) {
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
                Field* tmp_field;
                tmp_field = new Field(my_columns[i]->GetType());
                my_fields.push_back(*tmp_field);
            }
        }
        else {
            Field* tmp_field;
            if (tmp_type == kTypeInt) {
                string tmp_val = column_value->val_;
                for (uint32_t j = 0; j < tmp_val.length(); j++) {
                    if (tmp_val[j] < '0' && tmp_val[j] > '9' && tmp_val[j] != '-') {
                        //不是整数
                        return DB_FAILED;
                    }
                }
                tmp_field = new Field(kTypeInt, stoi(column_value->val_));
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
                tmp_field = new Field(kTypeFloat, tmp_float);
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
                tmp_field = new Field(kTypeChar, tmp_val, strlen(tmp_val), manage);
            }
            my_fields.push_back(*tmp_field);
        }
        i++;
        column_value = column_value->next_;
    }
    if (column_value != NULL) {
        std::cout << "Too many elements!" << std::endl;
    }
    Row* my_row = new Row(my_fields);
    // 主键/unique判断还没写
    // 插入引起的index更改不会写
    my_table_info->GetTableHeap()->InsertTuple(*my_row, nullptr);
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
    return DB_FAILED;
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