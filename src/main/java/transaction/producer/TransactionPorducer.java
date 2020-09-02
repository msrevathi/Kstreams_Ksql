package transaction.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionPorducer {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(ProducerConfig.CLIENT_ID_CONFIG, TransactionProducerAppConfigs.applicationID);
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TransactionProducerAppConfigs.bootstrapServers);
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TransactionProducerAppConfigs.transaction_id);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer kafkaProducer = new KafkaProducer<>(config);
        kafkaProducer.initTransactions();

        kafkaProducer.beginTransaction();
        // 1st transaction
        try {
            for (int i = 1; i <= TransactionProducerAppConfigs.numEvents; i++) {
                kafkaProducer.send(new ProducerRecord<>(TransactionProducerAppConfigs.topicName1, i, "value of message-T1 " + i));
                kafkaProducer.send(new ProducerRecord<>(TransactionProducerAppConfigs.topicName2, i, "value of message-T1 " + i));
            }
            kafkaProducer.commitTransaction();
        } catch (Exception e) {
            kafkaProducer.abortTransaction();
            throw new RuntimeException(e);

        }
        // 2nd transaction
        kafkaProducer.beginTransaction();
        try {
            for (int i = 1; i <= TransactionProducerAppConfigs.numEvents; i++) {
                kafkaProducer.send(new ProducerRecord<>(TransactionProducerAppConfigs.topicName1, i, "value of message-T2 " + i));
                kafkaProducer.send(new ProducerRecord<>(TransactionProducerAppConfigs.topicName2, i, "value of message-T2 " + i));
            }
            kafkaProducer.abortTransaction();
        } catch (Exception e) {
            kafkaProducer.abortTransaction();
            throw new RuntimeException(e);

        }
    }
}
