package me.ivanlis.example.bank.balance;

import static me.ivanlis.example.utils.Utils.createProducerProperties;

import com.google.gson.Gson;
import java.math.BigDecimal;
import java.sql.Time;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import me.ivanlis.example.bank.balance.messages.Transaction;
import me.ivanlis.example.bank.balance.serializers.TransactionDeserializer;
import me.ivanlis.example.bank.balance.serializers.TransactionSerializer;
import me.ivanlis.example.utils.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Serialized;

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
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransactionSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // must be set to all for indopotent
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1"); // flush message every ms
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");


        final KafkaProducer<String, Transaction> kafkaProducer = new KafkaProducer<>(properties);

        final int nMinutes = 1;
        final long finishTime = System.currentTimeMillis() + nMinutes * 60 * 1000; // current time + nMinutes

        while (finishTime > System.currentTimeMillis()) {
            String customerName = CUSTOMER_NAMES[RANDOM_GENERATOR.nextInt(CUSTOMER_NAMES.length)];

            TimeUnit.SECONDS.sleep(10);

            Transaction transaction = new Transaction(customerName, new BigDecimal(ThreadLocalRandom.current().nextInt(0, 100)));

            kafkaProducer.send(new ProducerRecord<>(TRANSACTION_TOPIC, customerName, transaction));
        }



    }

}
