package me.ivanlis.streaming.example.enricher.consumers;

import static me.ivanlis.streaming.example.utils.Utils.createCustomConsumerProperties;

import java.util.Arrays;
import java.util.Properties;
import me.ivanlis.streaming.example.enricher.messages.User;
import me.ivanlis.streaming.example.enricher.producers.UserDataProducer;
import me.ivanlis.streaming.example.enricher.serialisers.UserSerde.UserDeserialiser;
import me.ivanlis.streaming.example.utils.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

public class UserDataConsumer {

    private static final String GROUP_ID = "userDataConsumer";

    public static void main(String[] args) {
        Properties consumerProperties = createCustomConsumerProperties(
                GROUP_ID,
                Constants.BROKER,
                IntegerDeserializer.class.getName(),
                UserDeserialiser.class.getName()
        );

        KafkaConsumer<Integer, User> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Arrays.asList(UserDataProducer.TOPIC));

        while (true) {
            ConsumerRecords<Integer, User> records = consumer.poll(1000);
            records.forEach(record -> {
                System.out.println("Key: " + record.key());
                System.out.println("Value: " + record.value() + "\n\n");
            });

        }
    }
}
