package serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import types.*;

import java.util.HashMap;
import java.util.Map;

public class CustomSerdes extends Serdes {
    static public final class PosInvoiceSerde extends WrapperSerde<PosInvoice> {
        public PosInvoiceSerde() {
            super(new JsonSerializer(), new JsonDeserializer<>());
        }
    }

    public static Serde<PosInvoice> PosInvoice() {
        PosInvoiceSerde serde = new PosInvoiceSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static public final class DeliveryAddressSerde extends WrapperSerde<DeliveryAddressSerde> {
        public DeliveryAddressSerde() {
            super(new JsonSerializer(), new JsonDeserializer<>());
        }
    }

    public static Serde<DeliveryAddressSerde> DeliveryAddress() {
        DeliveryAddressSerde serde = new DeliveryAddressSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, DeliveryAddressSerde.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static public final class NotificationSerde extends WrapperSerde<Notification> {
        public NotificationSerde() {
            super(new JsonSerializer(), new JsonDeserializer<>());
        }
    }

    public static Serde<Notification> Notification() {
        NotificationSerde serde = new NotificationSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Notification.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static public final class HadoopRecordSerde extends WrapperSerde<HadoopRecord> {
        public HadoopRecordSerde() {
            super(new JsonSerializer(), new JsonDeserializer<>());
        }
    }

    public static Serde<HadoopRecord> HadoopRecord() {
        HadoopRecordSerde serde = new HadoopRecordSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, HadoopRecord.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static public final class SimpleInvoiceSerde extends WrapperSerde<SimpleInvoice> {
        public SimpleInvoiceSerde() {
            super(new JsonSerializer(), new JsonDeserializer<>());
        }
    }

    public static Serde<SimpleInvoice> SimpleInvoice() {
        SimpleInvoiceSerde serde = new SimpleInvoiceSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, SimpleInvoice.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class HeartBeatSerde extends WrapperSerde<HeartBeat> {
        HeartBeatSerde() {
            super(new JsonSerializer(), new JsonDeserializer<>());
        }
    }

    public static Serde<HeartBeat> HeartBeat() {
        HeartBeatSerde serde = new HeartBeatSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, HeartBeat.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

    static final class UserClicksSerde extends WrapperSerde<UserClicks> {
        UserClicksSerde() {
            super(new JsonSerializer(), new JsonDeserializer<>());
        }
    }

    public static Serde<UserClicks> UserClicks() {
        UserClicksSerde serde = new UserClicksSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, UserClicks.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }
}
