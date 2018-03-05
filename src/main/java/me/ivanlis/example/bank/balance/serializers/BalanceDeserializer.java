package me.ivanlis.example.bank.balance.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import java.nio.charset.Charset;
import java.util.Map;
import me.ivanlis.example.bank.balance.messages.Balance;
import org.apache.kafka.common.serialization.Deserializer;

public class BalanceDeserializer implements Deserializer<Balance> {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    private final Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Balance deserialize(String topic, byte[] data) {
        String balance = new String(data, CHARSET);
        return gson.fromJson(balance, Balance.class);
    }


    @Override
    public void close() {

    }
}
