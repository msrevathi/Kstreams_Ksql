package event.enrich.streamjoin;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class EventEnrichStreamApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "enrich-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();
        GlobalKTable<Object, Object> user_data = builder.globalTable("user-event");
        KStream<Object, Object> user_purchase = builder.stream("user-purchase");

        KStream<Object, String> user_purchased_items_join = user_purchase.join(user_data, (key, value) -> key,
                (user_info, user_purchase_info) -> "purchased:" + user_purchase_info + " user_info= " + user_info);
        user_purchased_items_join.to("user_purchased_items_join");

        KStream<Object, String> user_purchased_items_leftjoin = user_purchase.leftJoin(user_data, (key, value) -> key,
                (user_info, user_purchase_info) -> {
                    if (user_info != null)
                        return "purchased:" + user_purchase_info + " user_info= " + user_info;
                    else {
                        return "purchased:" + user_purchase_info + " user_info=null";
                    }
                });
        user_purchased_items_leftjoin.to("user-purchases-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
