package me.ivanlis.example.enricher.serialisers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.nio.charset.Charset;
import java.util.Map;
import me.ivanlis.example.enricher.messages.Address;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class AddressSerde implements Serde <Address> {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final GsonBuilder GSON_BUILDER = new GsonBuilder().serializeNulls();
    private static final Gson GSON = GSON_BUILDER.create();

    private final AddressSerialiser serialiser = new AddressSerialiser();
    private final AddressDeserialiser deserialiser = new AddressDeserialiser();

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
    public Serializer<Address> serializer() {
        return serialiser;
    }

    @Override
    public Deserializer<Address> deserializer() {
        return deserialiser;
    }

    public static class AddressSerialiser implements Serializer<Address> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, Address data) {
            return GSON.toJson(data).getBytes(CHARSET);
        }

        @Override
        public void close() {

        }
    }

    public static class AddressDeserialiser implements Deserializer<Address> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public Address deserialize(String topic, byte[] data) {
            return GSON.fromJson(new String(data, CHARSET), Address.class);
        }

        @Override
        public void close() {

        }
    }

    public static AddressSerde addressSerde() {
        return new AddressSerde();
    }
}
