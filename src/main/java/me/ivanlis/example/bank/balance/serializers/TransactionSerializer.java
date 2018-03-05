package me.ivanlis.example.bank.balance.serializers;

import com.google.gson.Gson;
import java.nio.charset.Charset;
import java.util.Map;
import me.ivanlis.example.bank.balance.messages.Transaction;
import org.apache.kafka.common.serialization.Serializer;

public class TransactionSerializer implements Serializer<Transaction> {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    private final Gson gson = new Gson();

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Transaction data) {
        return gson.toJson(data).getBytes(CHARSET);
    }

    @Override
    public void close() {

    }
}
