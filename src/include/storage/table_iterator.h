#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;

class TableIterator {

public:
  explicit TableIterator(BufferPoolManager *buffer_pool_manager, Schema *schema, LogManager *log_manager,
LockManager *lock_manager, RowId row_record);

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_;
  Schema *schema_;
  LogManager *log_manager_;
  LockManager *lock_manager_;
  Transaction *txn_;
  RowId row_record_; //
  Row tuple; // the current tuple
};

#endif //MINISQL_TABLE_ITERATOR_H
