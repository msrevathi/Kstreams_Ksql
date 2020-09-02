package bank.balance;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class jsonStreamDeSerialiser implements Deserializer {


    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        return null;
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        if (data == null) return null;
        else {
            try {
                objectMapper.readTree(data);
                return data;
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        }
    }

    @Override
    public void close() {

    }
}
