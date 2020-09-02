rem kafka-console-producer.bat --broker-list localhost:9092  --topic favourite-color-output-topic
rem kafka-console-producer.bat --broker-list localhost:9092 --topic favourite-color-input-topic  property parse.key=true --property key.separator=,
rem kafka-console-producer.bat --broker-list localhost:9092 --topic favourite-color-input-topic
rem kafka-console-producer.bat --broker-list localhost:9092 --topic bank-transaction-input-topic

kafka-console-producer.bat --broker-list localhost:9092 --topic simple-invoice --property parse.key=true --property key.separator=":"
