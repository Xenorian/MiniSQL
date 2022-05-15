#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"
#include "buffer/buffer_pool_manager.h"
#include "transaction/log_manager.h"
#include "transaction/lock_manager.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
 explicit TableIterator(RowId record_id_first, BufferPoolManager *buffer_pool_manager, Schema *schema,
                        LogManager *log_manager, LockManager *lock_manager);

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const { return (this->record_now_.GetRowId() == itr.record_now_.GetRowId()); };

  inline bool operator!=(const TableIterator &itr) const {
    return (this->record_now_.GetRowId().Get() != itr.record_now_.GetRowId().Get());
  };

  TableIterator &operator=(const TableIterator &other);

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
 Transaction *txn;
 //RowId record_id_now_;
 Row record_now_;
};

#endif //MINISQL_TABLE_ITERATOR_H
