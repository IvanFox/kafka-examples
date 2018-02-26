package me.ivanlis.example.bank.balance;

import static me.ivanlis.example.utils.Utils.createProducerProperties;

import com.google.gson.Gson;
import java.math.BigDecimal;
import java.util.Random;
import me.ivanlis.example.bank.balance.messages.Transaction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TransactionProducer {

    public static final String TRANSACTION_TOPIC = "transactions";

    private static final String[] CUSTOMER_NAMES = {
            "James Doe",
            "James Rodriguez",
            "Sanctioned man",
            "Good Guy",
            "Adam Smith",
            "Karina Star"
    };

    private static final Random RANDOM_GENERATOR = new Random();

    private static final Gson GSON = new Gson();

    public static void main(String[] args) {

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(createProducerProperties("localhost:9092"));

        long finishTime = System.currentTimeMillis() + 2 * 60 * 1000; // current time + 2 minutes

        while (finishTime > System.currentTimeMillis()) {
            String customerName = CUSTOMER_NAMES[RANDOM_GENERATOR.nextInt(CUSTOMER_NAMES.length)];
            Transaction transaction = new Transaction(customerName, new BigDecimal(RANDOM_GENERATOR.nextInt(Integer.MAX_VALUE)));
            kafkaProducer.send(new ProducerRecord<>(TRANSACTION_TOPIC, customerName, GSON.toJson(transaction)));
        }



    }

}
