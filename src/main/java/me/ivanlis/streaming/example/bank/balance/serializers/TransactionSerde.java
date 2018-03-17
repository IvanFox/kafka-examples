package me.ivanlis.streaming.example.bank.balance.serializers;

import java.util.Map;
import me.ivanlis.streaming.example.bank.balance.messages.Transaction;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class TransactionSerde implements Serde<Transaction> {

    private final TransactionSerializer serializer = new TransactionSerializer();
    private final TransactionDeserializer deserializer = new TransactionDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<Transaction> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Transaction> deserializer() {
        return deserializer;
    }
}
