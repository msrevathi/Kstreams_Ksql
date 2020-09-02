package pos.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import types.PosInvoice;
import serdes.CustomSerdes;

import java.util.Properties;

public class posStreamApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "pos-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PosInvoice> inputStream = builder.stream("pos-topic", Consumed.with(CustomSerdes.String(), CustomSerdes.PosInvoice()));

        inputStream.filter(((key, value) -> value.getDeliveryType().equals("HOME-DELIVERY")))
                .to("shipment-topic", Produced.with(Serdes.String(), CustomSerdes.PosInvoice()));

        inputStream.filter(((key, value) -> value.getCustomerType().equals("PRIME")))
                .mapValues(invoice -> RecordBuilder.getNotification(invoice))
                .to("loyalty-topic", Produced.with(Serdes.String(), CustomSerdes.Notification()));

        inputStream.mapValues(value -> RecordBuilder.getMaskedInvoice(value))
                .flatMapValues(value -> RecordBuilder.getHadoopRecords(value))
                .to("hadoop-sink", Produced.with(Serdes.String(), CustomSerdes.HadoopRecord()));
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
