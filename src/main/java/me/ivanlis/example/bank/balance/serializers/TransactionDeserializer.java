package me.ivanlis.example.bank.balance.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import java.nio.charset.Charset;
import java.util.Map;
import me.ivanlis.example.bank.balance.messages.Transaction;
import org.apache.kafka.common.serialization.Deserializer;

public class TransactionDeserializer implements Deserializer<Transaction> {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    private final Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Transaction deserialize(String topic, byte[] data) {
        String transaction = new String(data, CHARSET);
        return gson.fromJson(transaction, Transaction.class);
    }


    @Override
    public void close() {

    }
}
