package me.ivanlis.streaming.example.enricher.serialisers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.nio.charset.Charset;
import java.util.Map;
import me.ivanlis.streaming.example.enricher.messages.User;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class UserSerde implements Serde<User> {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final GsonBuilder GSON_BUILDER = new GsonBuilder().serializeNulls();
    private static final Gson GSON = GSON_BUILDER.create();

    private final UserSerialiser serialiser = new UserSerialiser();
    private final UserDeserialiser deserialiser = new UserDeserialiser();

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
    public Serializer<User> serializer() {
        return serialiser;
    }

    @Override
    public Deserializer<User> deserializer() {
        return deserialiser;
    }

    public static class UserSerialiser implements Serializer<User> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, User data) {
            return GSON.toJson(data).getBytes(CHARSET);
        }

        @Override
        public void close() {

        }
    }

    public static class UserDeserialiser implements Deserializer<User> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public User deserialize(String topic, byte[] data) {
            String userData = new String(data, CHARSET);
            return GSON.fromJson(userData, User.class);
        }

        @Override
        public void close() {

        }
    }

    public static UserSerde userSerde() {
        return new UserSerde();
    }
}
