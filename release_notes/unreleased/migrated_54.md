## fix

- Fixed CAS conflict detection for globally distributed tables with secondary
  unique indexes. Previously, stale CAS requests could be appended to the raft
  log with different primary keys but the same secondary unique key, causing
  replicas to fail applying the later entry and preventing raft from advancing.
  Such requests are now rejected with a retriable `CasConflictFound` error.
