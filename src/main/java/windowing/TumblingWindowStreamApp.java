package windowing;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import serdes.CustomSerdes;
import types.SimpleInvoice;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Properties;

public class TumblingWindowStreamApp {
    private static final Logger logger = LogManager.getLogger(TumblingWindowStreamApp.class);

   /* public static Topology tumbleWindowTopology() {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, SimpleInvoice> input_stream = builder.stream("simple-invoice-topic",
                Consumed.with(CustomSerdes.String(), CustomSerdes.SimpleInvoice())
                        .withTimestampExtractor(new SimpleInvoiceTimeExtractor()));


        KTable<Windowed<String>, Long> invoice_count = input_stream.groupByKey(Grouped.with(CustomSerdes.String(), CustomSerdes.SimpleInvoice()))
                //.windowedBy(TimeWindows.of(Duration.ofMinutes(5)).grace(Duration.ofMinutes(2))) //-with grace period 2 minutes
                //.windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1))) //-for hoping window
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5))) //tumbling window
                .count();
        invoice_count.toStream().foreach(
                (key, value) ->
                        logger.info(
                                "Store ID: " + key.key() + " Window ID: " + key.window().hashCode() +
                                        " Window start: " + Instant.ofEpochMilli(key.window().start()).atOffset(ZoneOffset.UTC) +
                                        " Window end: " + Instant.ofEpochMilli(key.window().end()).atOffset(ZoneOffset.UTC) +
                                        " Count: " + value
                        ));
        return builder.build();

    }

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "tumbling-window-app");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(StreamsConfig.STATE_DIR_CONFIG, "tmp/state_store");
        prop.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        TumblingWindowStreamApp tumblingWindowStreamApp = new TumblingWindowStreamApp();
        KafkaStreams streams = new KafkaStreams(tumblingWindowStreamApp.tumbleWindowTopology(), prop);


        //OR
        // KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();
        //4.Shutdown the stream app gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        logger.info("Stopping Streams");
    }*/

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tumbling-window-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "tmp/state_store");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, SimpleInvoice> KS0 = streamsBuilder.stream("simple-invoice",
                Consumed.with(CustomSerdes.String(), CustomSerdes.SimpleInvoice())
                        .withTimestampExtractor(new SimpleInvoiceTimeExtractor())
        );

        KTable<Windowed<String>, Long> KT0 = KS0.groupByKey(Grouped.with(CustomSerdes.String(), CustomSerdes.SimpleInvoice()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                .count();

        KT0.toStream().foreach(
                (wKey, value) -> logger.info(
                        "Store ID: " + wKey.key() + " Window ID: " + wKey.window().hashCode() +
                                " Window start: " + Instant.ofEpochMilli(wKey.window().start()).atOffset(ZoneOffset.UTC) +
                                " Window end: " + Instant.ofEpochMilli(wKey.window().end()).atOffset(ZoneOffset.UTC) +
                                " Count: " + value
                )
        );

//         KT0.toStream().to("",Produced.with());

//        KStream<Windowed<String>, Long> windowedLongKStream = KT0.toStream();
//
//        windowedLongKStream.print((Printed.<Windowed<String>, Long>toSysOut().withLabel("tumbling ")));

//        streamsBuilder.stream("simple-invoice")
//               // .map((key, value) -> new KeyValue<>(value.().toString(), rating))
//                .groupByKey()
//                .windowedBy(TimeWindows.of(Duration.ofMinutes(10)))
//                .count()
//                .toStream()
//                .map((Windowed<String> key, Long count) -> new KeyValue(key.key(), count.toString()))
//                .to("", Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));
    }
}
