package me.ivanlis.example.enricher.consumers;

import static me.ivanlis.example.enricher.serialisers.PhoneNumberSerde.*;
import static me.ivanlis.example.utils.Utils.createCustomConsumerProperties;

import java.util.Arrays;
import java.util.Properties;
import me.ivanlis.example.enricher.messages.Address;
import me.ivanlis.example.enricher.messages.PhoneNumber;
import me.ivanlis.example.enricher.producers.PhoneNumberProducer;
import me.ivanlis.example.enricher.producers.UserDataProducer;
import me.ivanlis.example.enricher.serialisers.AddressSerde.AddressDeserialiser;
import me.ivanlis.example.enricher.serialisers.PhoneNumberSerde;
import me.ivanlis.example.utils.Constants;
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
