package me.ivanlis.streaming.example.enricher.consumers;

import static me.ivanlis.streaming.example.enricher.serialisers.PhoneNumberSerde.*;
import static me.ivanlis.streaming.example.utils.Utils.createCustomConsumerProperties;

import java.util.Arrays;
import java.util.Properties;
import me.ivanlis.streaming.example.enricher.messages.PhoneNumber;
import me.ivanlis.streaming.example.enricher.producers.PhoneNumberProducer;
import me.ivanlis.streaming.example.utils.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

public class PhoneNumberConsumer {
    private static final String GROUP_ID = "phoneNumberConsumer";

    public static void main(String[] args) {
        Properties consumerProperties = createCustomConsumerProperties(
                GROUP_ID,
                Constants.BROKER,
                IntegerDeserializer.class.getName(),
                PhoneNumberDeserialiser.class.getName()
        );

        KafkaConsumer<Integer, PhoneNumber> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Arrays.asList(PhoneNumberProducer.TOPIC));

        while (true) {
            ConsumerRecords<Integer, PhoneNumber> records = consumer.poll(1000);
            records.forEach(record -> {
                System.out.println("Key: " + record.key());
                System.out.println("Value: " + record.value() + "\n\n");
            });

        }
    }
}
