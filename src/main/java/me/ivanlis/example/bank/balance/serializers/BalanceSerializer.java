package me.ivanlis.example.bank.balance.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import java.nio.charset.Charset;
import java.util.Map;
import me.ivanlis.example.bank.balance.messages.Balance;
import org.apache.kafka.common.serialization.Serializer;

public class BalanceSerializer implements Serializer<Balance> {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    private final Gson gson = new Gson();

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Balance data) {
        String balance = gson.toJson(data);
        return balance.getBytes(CHARSET);
    }

    @Override
    public void close() {

    }

}
