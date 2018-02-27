package me.ivanlis.example.bank.balance;

import static me.ivanlis.example.utils.Utils.createProducerProperties;

import com.google.gson.Gson;
import java.math.BigDecimal;
import java.sql.Time;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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


    public static void main(String[] args) throws InterruptedException {

        final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(createProducerProperties("localhost:9092"));

        final int nMinutes = 1;
        final long finishTime = System.currentTimeMillis() + nMinutes * 60 * 1000; // current time + nMinutes

        while (finishTime > System.currentTimeMillis()) {
            String customerName = CUSTOMER_NAMES[RANDOM_GENERATOR.nextInt(CUSTOMER_NAMES.length)];
            TimeUnit.SECONDS.sleep(1);
            Transaction transaction = new Transaction(customerName, new BigDecimal(RANDOM_GENERATOR.nextInt(Integer.MAX_VALUE)));
            kafkaProducer.send(new ProducerRecord<>(TRANSACTION_TOPIC, customerName, GSON.toJson(transaction)));
        }



    }

}
