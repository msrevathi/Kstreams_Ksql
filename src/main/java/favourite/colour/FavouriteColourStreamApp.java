package favourite.colour;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

public class FavouriteColourStreamApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fav-color");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("favourite-color-input-topic");

        ArrayList<String> colours;
        colours = new ArrayList<String>(Arrays.asList("red", "blue", "green"));

        KStream<String, String> user_colors = stream.filter(((key, value) -> value.contains(",")))
                .selectKey((k, v) -> v.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((x, y) -> colours.contains(y));

        user_colors.to("fav-color-intermidiate-topic");
        KTable<String, String> fav_color = builder.table("fav-color-intermidiate-topic");

        KTable<String, Long> fav_color_count = fav_color.groupBy((k, v) -> KeyValue.pair(v, v))
                .count();

        fav_color_count.toStream().to("favourite-color-output-topic",
                Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams stream_start = new KafkaStreams(builder.build(), config);
        stream_start.cleanUp();
        stream_start.start();
        Runtime.getRuntime().addShutdownHook(new Thread(stream_start::close));
    }

}


