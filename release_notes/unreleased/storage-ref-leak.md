## fix/sql

- An expired storage reference of a distributed SQL query no longer blocks
  bucket rebalancing. If a router took a reference on a storage and crashed
  before the query went on, the storage could neither send nor receive buckets
  until its restart. The reference now expires after the query timeout, as the
  references of vshard itself do.
