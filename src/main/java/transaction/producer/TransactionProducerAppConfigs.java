package transaction.producer;

class TransactionProducerAppConfigs {
    final static String applicationID = "transaction-producer";
    final static String transaction_id = "transaction-producer";
    final static String bootstrapServers = "localhost:9092,localhost:9093";
    final static String topicName1 = "transaction-1";
    final static String topicName2 = "transaction-2";
    final static int numEvents = 2;
}

