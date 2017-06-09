#pragma once
#ifndef MICA_TRANSACTION_LOGGING_H_
#define MICA_TRANSACTION_LOGGING_H_

#include "mica/common.h"
#include "mica/transaction/db.h"
#include "mica/transaction/row.h"
#include "mica/transaction/row_version_pool.h"
#include "mica/transaction/context.h"
#include "mica/transaction/transaction.h"

namespace mica {
namespace transaction {
template <class StaticConfig>
class LoggerInterface {
 public:
  bool log(const Transaction<StaticConfig>* tx);
};

template <class StaticConfig>
class NullLogger : public LoggerInterface<StaticConfig> {
 public:
  bool log(const Transaction<StaticConfig>* tx) {
    (void)tx;
    return true;
  }
};
}
}

#endif