package me.ivanlis.example.enricher.serialisers;

import com.google.gson.Gson;
import java.nio.charset.Charset;
import java.util.Map;
import me.ivanlis.example.enricher.messages.PhoneNumber;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class PhoneNumberSerde implements Serde<PhoneNumber> {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final Gson GSON = new Gson();

    private final PhoneNumberSerialiser serialiser = new PhoneNumberSerialiser();
    private final PhoneNumberDeserialiser deserialiser = new PhoneNumberDeserialiser();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serialiser.configure(configs, isKey);
        deserialiser.configure(configs, isKey);
    }

    @Override
    public void close() {
        serialiser.close();
        deserialiser.close();
    }

    @Override
    public Serializer<PhoneNumber> serializer() {
        return serialiser;
    }

    @Override
    public Deserializer<PhoneNumber> deserializer() {
        return deserialiser;
    }


    public static class PhoneNumberSerialiser implements Serializer<PhoneNumber> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, PhoneNumber data) {
            return GSON.toJson(data).getBytes(CHARSET);
        }

        @Override
        public void close() {

        }
    }

    public static class PhoneNumberDeserialiser implements Deserializer<PhoneNumber> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public PhoneNumber deserialize(String topic, byte[] data) {
            String phoneNumber = new String(data, CHARSET);
            return GSON.fromJson(phoneNumber, PhoneNumber.class);
        }

        @Override
        public void close() {

        }
    }
}
