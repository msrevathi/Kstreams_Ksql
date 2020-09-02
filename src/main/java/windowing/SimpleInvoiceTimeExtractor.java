package windowing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import types.SimpleInvoice;

import java.time.Instant;

public class SimpleInvoiceTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {

        SimpleInvoice value = (SimpleInvoice) record.value();
        String createdTime = value.getCreatedTime();
        long event_createdTime_epoch = Instant.parse(createdTime).toEpochMilli();
        return ((event_createdTime_epoch > 0) ? event_createdTime_epoch : partitionTime);
    }
}
