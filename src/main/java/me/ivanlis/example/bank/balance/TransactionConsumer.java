package me.ivanlis.example.bank.balance;

import static java.util.Arrays.asList;
import static me.ivanlis.example.utils.Utils.createDefaultConsumerProperties;

import java.util.Properties;
import lombok.val;
import me.ivanlis.example.utils.Constants;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TransactionConsumer {

    private static final String GROUP_ID = "transactionConsumer";

    public static void main(String[] args) {
        Properties consumerProperties = createDefaultConsumerProperties(GROUP_ID, Constants.BROKER);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);

        kafkaConsumer.subscribe(asList(TransactionProducer.TRANSACTION_TOPIC));

        while (true) {
            val consumerRecords = kafkaConsumer.poll(100);
            consumerRecords.forEach(record -> System.out.println(record.value()));
        }
    }
}
