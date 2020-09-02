package bank.balance;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankBalanceJsonProducer {
    static String input_topic = "bank-transaction-input-topic";

    /**
     * this is implement using exactly once symantic
     *https://slower.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/15307178#questions
     * @param args
     * Messges are produced to the kafka broker by the producer only once by setting ENABLE_IDEMPOTENCE_CONFIG to true
     * to avoid sending duplicates.
     * How this will work?
     * Producer will perorm two thing on high level
     * u1.Internal id for producer instance:Producer will perform handshake with broker and request for unique producer id(broker will dynamically assign unique id to each producer)
     * 2.Message sequence Number:Producer api will Sequence number to each message (start from 0 and creased monotonically increases per partition)
     * When io thread send messages the messages to the leader ,the message is uniquely identified by the producer bu producer_id+sequence_number.
     * Hence broker will be able to identify duplicates.(1st message x, next message x+1 ....)
     * This will allow to ensure message are neither duplicated not lost
     */
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 100000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100000);

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        int i = 0;
        while (true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(jsonEventTransaction("john"));
                Thread.sleep(100);
                producer.send(jsonEventTransaction("stephane"));
                Thread.sleep(100);
                producer.send(jsonEventTransaction("alice"));
                Thread.sleep(100);
                i += 1;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();

    }

    public static ProducerRecord jsonEventTransaction(String name) {
        //create an empty json object
        ObjectNode transaction_even = JsonNodeFactory.instance.objectNode();

        // generation random amount
        int amount = ThreadLocalRandom.current().nextInt(0, 100);
        //time
        Instant time = Instant.now();
        //add time and amount to the json object
        transaction_even.put("name", name);
        transaction_even.put("amount", amount);
        transaction_even.put("time", time.toString());
        return new ProducerRecord<>(input_topic, name, transaction_even.toString());

    }
}

