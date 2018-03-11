package me.ivanlis.example.bank.balance;

import static me.ivanlis.example.utils.Utils.*;

import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import me.ivanlis.example.bank.balance.messages.Transaction;
import me.ivanlis.example.bank.balance.serializers.TransactionSerializer;
import me.ivanlis.example.utils.Constants;
import me.ivanlis.example.utils.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

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

    public static void main(String[] args) throws InterruptedException {
        final KafkaProducer<String, Transaction> kafkaProducer = new KafkaProducer<>(createExactlyOnceProducer(
                Constants.BROKER,
                StringSerializer.class.getName(),
                TransactionSerializer.class.getName()
                )
        );

        final int nMinutes = 1;
        final long finishTime = System.currentTimeMillis() + nMinutes * 60 * 1000; // current time + nMinutes

        while (finishTime > System.currentTimeMillis()) {
            String customerName = CUSTOMER_NAMES[RANDOM_GENERATOR.nextInt(CUSTOMER_NAMES.length)];

            TimeUnit.SECONDS.sleep(2);

            Transaction transaction = new Transaction(customerName, new BigDecimal(ThreadLocalRandom.current().nextInt(0, 100)));

            kafkaProducer.send(new ProducerRecord<>(TRANSACTION_TOPIC, customerName, transaction));
        }



    }

}
