package me.ivanlis.example.enricher.consumers;

import static me.ivanlis.example.utils.Utils.createCustomConsumerProperties;

import java.util.Arrays;
import java.util.Properties;
import me.ivanlis.example.enricher.UserEnricherApp;
import me.ivanlis.example.enricher.messages.User;
import me.ivanlis.example.enricher.serialisers.UserSerde.UserDeserialiser;
import me.ivanlis.example.utils.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

public class AddressJoinConsumer {

    private static final String GROUP_ID = "addressJoinConsumer1";

    public static void main(String[] args) {
        Properties consumerProperties = createCustomConsumerProperties(
                GROUP_ID,
                Constants.BROKER,
                IntegerDeserializer.class.getName(),
                UserDeserialiser.class.getName()
        );

        KafkaConsumer<Integer, User> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Arrays.asList(UserEnricherApp.USER_ADDRESS_JOIN));

        while (true) {
            ConsumerRecords<Integer, User> records = consumer.poll(1000);
            records.forEach(record -> {
                System.out.println("Key: " + record.key());
                System.out.println("Value: " + record.value() + "\n\n");
            });

        }
    }

}
