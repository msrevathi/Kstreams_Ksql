package windowing;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import serdes.CustomSerdes;
import types.HeartBeat;
import types.SimpleInvoice;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Properties;

public class SupressWindowStreamApp {
    private static final Logger logger = LogManager.getLogger();

    public static Topology tumbleWindowTopology() {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, HeartBeat> input_stream = builder.stream("alerts-topic",
                Consumed.with(CustomSerdes.String(), CustomSerdes.HeartBeat())
                        .withTimestampExtractor(new SimpleInvoiceTimeExtractor()));


        KTable<Windowed<String>, Long> invoice_count = input_stream.groupByKey(Grouped.with(CustomSerdes.String(), CustomSerdes.HeartBeat()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
        invoice_count.toStream().foreach(
                (key, value) ->
                        logger.info(
                                "Store ID: " + key.key() + " Window ID: " + key.window().hashCode() +
                                        " Window start: " + Instant.ofEpochMilli(key.window().start()).atOffset(ZoneOffset.UTC) +
                                        " Window end: " + Instant.ofEpochMilli(key.window().end()).atOffset(ZoneOffset.UTC) +
                                        " Count: " + value +
                                        (value > 2 ? " Application is Alive" : " Application Failed - Sending Alert Email...")
                        ));
        return builder.build();

    }

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "supress-window-app");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.STATE_DIR_CONFIG, "tmp/state_store");
        prop.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        SupressWindowStreamApp tumblingWindowStreamApp = new SupressWindowStreamApp();
        KafkaStreams streams = new KafkaStreams(tumblingWindowStreamApp.tumbleWindowTopology(), prop);


        //OR
        // KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();
        //4.Shutdown the stream app gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
