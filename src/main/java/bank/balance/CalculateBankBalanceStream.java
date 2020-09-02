package bank.balance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;

public class CalculateBankBalanceStream {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //enable Exactly once process
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 100000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100000);

        //define json serdes
        JsonSerializer jsonSerializer = new JsonSerializer();
        JsonDeserializer jsonDeserializer = new JsonDeserializer();
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> stream = builder.stream("bank-transaction-input-topic", Consumed.with(Serdes.String(), jsonSerde));

        //create initial json node object
        ObjectNode defaultTranscation = JsonNodeFactory.instance.objectNode();
        defaultTranscation.put("count", 0);
        defaultTranscation.put("balance", 0);
        defaultTranscation.put("time", Instant.ofEpochMilli(0L).toString());


        KTable<String, JsonNode> aggregate_res = stream
                .groupByKey()
                .aggregate(
                        () -> defaultTranscation,
                        (key, transaction, balance) -> calculateBalance(transaction, balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );

        aggregate_res.toStream().to("bank-balance-out-topic1", Produced.with(Serdes.String(), jsonSerde));
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

        private static JsonNode calculateBalance(JsonNode transaction, JsonNode balance) {
            // create a new balance json object
            ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
            newBalance.put("count", balance.get("count").asInt() + 1);
            newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

            Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
            Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
            Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
            newBalance.put("time", newBalanceInstant.toString());
            return newBalance;
        }
}


