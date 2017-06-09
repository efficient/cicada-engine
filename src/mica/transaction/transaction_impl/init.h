#pragma once
#ifndef MICA_TRANSACTION_TRANSACTION_IMPL_INIT_H_
#define MICA_TRANSACTION_TRANSACTION_IMPL_INIT_H_

namespace mica {
namespace transaction {
template <class StaticConfig>
Transaction<StaticConfig>::Transaction(Context<StaticConfig>* ctx)
    : ctx_(ctx), began_(false) {
  last_commit_time_ = 0;

  access_buckets_.resize(StaticConfig::kAccessBucketRootCount);

  consecutive_commits_ = 0;
}

template <class StaticConfig>
Transaction<StaticConfig>::~Transaction() {
  if (began_) abort();
}
}
}

#endif