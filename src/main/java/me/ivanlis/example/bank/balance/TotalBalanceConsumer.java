package me.ivanlis.example.bank.balance;

import static java.util.Arrays.asList;
import static me.ivanlis.example.utils.Utils.createConsumerProperties;

import java.util.Properties;
import lombok.val;
import me.ivanlis.example.bank.balance.messages.Balance;
import me.ivanlis.example.bank.balance.serializers.BalanceDeserializer;
import me.ivanlis.example.bank.balance.serializers.BalanceSerde;
import me.ivanlis.example.utils.Constants;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class TotalBalanceConsumer {

    private static final String GROUP_ID = "totalBalanceCons1";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Constants.BROKER);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", GROUP_ID);
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000"); //every second the offset will be commited
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(asList(TotalBalanceAggregator.TOTAL_BALANCE_TOPIC));

        while (true) {
            val consumerRecords = kafkaConsumer.poll(100);
            consumerRecords.forEach(record -> System.out.println(
                    "Owner: " + record.key()  + "   Balance is: " + record.value())
            );
        }
    }
}
