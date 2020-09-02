package wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

public class ForConfluentStreamWordCount {
    public static Topology WordCountTopology() {
        /**
         * steps to build a stream App
         * 1.open a stream to source
         * 2.Process the stream
         * 3.create a topology
         * 4.Shutdown the stream app gracefully
         */
        StreamsBuilder builder = new StreamsBuilder();
        //1.open a stream to source
        KStream<String, String> textLines = builder.stream("word-count-input");
        //2.Process the stream
        KTable<String, Long> wordCounts = textLines
                .mapValues(textLine -> textLine.toLowerCase())
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                .selectKey((key, word) -> word)
                .groupByKey()
                .count();

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        /**
         *   3.create a topology :Kafka streams computational logic is called topology, represented by Topology class
         *   we can build all the computational logic into a Topology class by build method.
         *   builder:Java Builder pattern that allows step by step creation of complex objects using the correct sequence of actions.
         *   SO here we defined a series of activities for a topology. and called the build() to get the Topology object
         */

       // Topology topology = builder.build();
        return builder.build();
    }

    public static void main(String[] args) throws Exception {

        final Properties props = loadConfig(args[0]);

        //create stream properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.con_producerApplicationID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.con_bootstrapServers);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppConfigs.offset_status);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        /*
        Serdes:
        A kafka stream app will read as well as write the data.i.e i has to do both serialize and deserialize .
        They internally create a combination of producer and consumer .Hence they need Serders factory approach is followed
        by kafka streams,where both serialize and deserialize are implemented
        Instead of specifying both serializer and deserializer every time we can use Serdes.
         */
        ForConfluentStreamWordCount streamWordCount = new ForConfluentStreamWordCount();

        KafkaStreams streams = new KafkaStreams(streamWordCount.WordCountTopology(), config);

        //OR
        // KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();
        //4.Shutdown the stream app gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));



  /*      // Update:
        // print the topology every 10 seconds for learning purposes
        while (true) {
            System.out.println(streams.toString());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }*/


    }
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

}
