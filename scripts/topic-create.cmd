rem kafka-topics.bat --create --zookeeper localhost:2181 --topic favourite-color-input-topic --partitions 5 --replication-factor 3 --config segment.bytes=1000000
rem kafka-topics.bat --create --zookeeper localhost:2181 --topic favourite-color-output-topic --partitions 5 --replication-factor 3 --config cleanup.policy=compact
rem kafka-topics.bat --create --zookeeper localhost:2181 --topic fav-color-intermidiate-topic --partitions 5 --replication-factor 3 --config cleanup.policy=compact
rem kafka-topics.bat --create --zookeeper localhost:2181 --topic bank-balance-out-topic --partitions 1 --replication-factor 1 --config cleanup.policy=compact
rem kafka-topics.bat --create --zookeeper localhost:2181 --topic bank-balance-out-topic1 --partitions 1 --replication-factor 3 --config cleanup.policy=compact
rem kafka-topics.bat --create --zookeeper localhost:2181 --topic bank-balance-exactly-once --partitions 1 --replication-factor 3 --config cleanup.policy=compact
rem kafka-topics.bat --create --zookeeper localhost:2181 --topic hello-producer-topic --partitions 1 --replication-factor 3
rem kafka-topics.bat --create --zookeeper localhost:2181 --topic transaction-2 --partitions 1 --replication-factor 3

rem kafka-topics.bat --create --zookeeper localhost:2181 --topic user-purchase --partitions 3 --replication-factor 3
rem kafka-topics.bat --create --zookeeper localhost:2181 --topic user-event --partitions 3 --replication-factor 3
rem kafka-topics.bat --create --zookeeper localhost:2181 --topic user-purchases-enriched-left-join --partitions 3 --replication-factor 3
rem kafka-topics.bat --create --zookeeper localhost:2181 --topic user-purchases-enriched-inner-join --partitions 3 --replication-factor 3

kafka-topics.bat --create --zookeeper localhost:2181 --topic simple-invoice-output --partitions 3 --replication-factor 2



