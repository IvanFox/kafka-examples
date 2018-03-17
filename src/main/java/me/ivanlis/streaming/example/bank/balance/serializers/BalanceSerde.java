package me.ivanlis.streaming.example.bank.balance.serializers;

import java.util.Map;
import me.ivanlis.streaming.example.bank.balance.messages.Balance;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class BalanceSerde implements Serde<Balance> {

    private final BalanceSerializer serializer = new BalanceSerializer();
    private final BalanceDeserializer deserializer = new BalanceDeserializer();

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
    public Serializer<Balance> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Balance> deserializer() {
        return deserializer;
    }
}
