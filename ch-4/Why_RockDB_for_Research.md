# SQL, NoSQL, NewSQL and Streaming - How RockDB storage Engine is reshaping the Solution Architecture | Why I choose RockDB with Kafka Streaming as part of ongoing DataCenter Resource Optimisation Research

- [x] In 2018, Facebook replaced the InnoDB storage engine of their massive MySQL cluster with RockDB LMS, results in 50% storage saving for Facebook
- [x] Instagram's replace  the Apache Cassandra’s own Java written LSM tree with RocksDB, which is now default available to all other user’s of Apache Cassandra (Largest popular NoSQL db)
- [x] Underlying, RocksDB implements  log-structured merge tree aka LSM tree, an indexing structure optimized to handle high-volume—sequential or random—write workloads -> thus treating every write as an append operation, with compaction runs to invalidate keys or older version of data of valid keys. Thus compation creating fewer, larger and larger files.
- [x] By using bloom’s filter,  RockDB offers great read performance, thus ideal for Distributed database. Though some other popular DB storage engine is InnoDB implementing b+tree.
- [x] Though InnoDB (b+tree) perfor consistently well in read intensive benchmarks, when RockDB outperforming InnoDB at a large margin for write intensive benchmarks. This advantage, in relative terms, was not as big as the advantage RocksDB provides in the case of write-intensive tasks over InnoDB
- [x] [RockDB outperforming InnoDB in write] When the average insertion rate in InnoDB-5.7 engine in 1.9, for RockDB this is 1 at Intel i3 NUC. A similar is true for Intel i5 NUC.
- [x] [RockDB is close to InnoDB in read] When InnoDB-5.7 takes 508 seconds to scan all secondary index at Intel i3 NUC, rockDB takes 589 seconds.
- [x] RockDB is unable at different hardware configuration, and Facebook is also improving it managibility with language binding to C,C++ and Java.
- [x] Several DB variants already embedded RockDB as storage engine I.e. Apache Cassandra, CockroachDB, MySQL (MyRocks) and Rockset
- [x] Outside of Distributed Database realm, several mission-critical use cases where RockDB is widely used
    - [x] Kafka Stream - to build applications and microservices that consume and produce messages stored in Kafka clusters. Kafka Streams supports fault-tolerant stateful applications along with stateless referencing too. RocksDB is used by default to store state (aka State-store which could be exposed to outside world using Read replicas) in such configurations.
    - [x] Apache Samza - also used RockDB for their fault tolerant configurations.
    - [x] Netflix - picked RockDB for their SSD caching needs in their GlobalCaching system and EVCache.
    - [x] Santander UK - Streaming enrichment solution by Cloudera for real-time-transactional analysis using RockDB as state-store.
    - [x] Uber - Cherami, Uber’s own durable distributed messaging system equivalent to Amazon’s SQS. Choose RocksDB as their storage engine in their storage hosts for its performance and indexing features.
- [x] Where researchers are working to optimise RockDB when using in Kafka Streaming Application
    - [x] Dealing with duplicate keys and deletion entries (i.e., tombstones) that will require a full compaction in order to get an accurate number of keys from the RockDB endorsed state-stores at Kafka Streaming.
    - [x] Less accurate Key count when RockDB merge operations are performed, results in more compation need which could be costly and time consuming.
