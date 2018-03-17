package me.ivanlis.streaming.example.enricher.consumers;

import java.util.Arrays;
import java.util.Properties;
import me.ivanlis.streaming.example.enricher.messages.Address;
import me.ivanlis.streaming.example.enricher.producers.UserDataProducer;
import me.ivanlis.streaming.example.utils.Constants;
import me.ivanlis.streaming.example.enricher.serialisers.AddressSerde.AddressDeserialiser;
import me.ivanlis.streaming.example.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

public class AddressConsumer {

    private static final String GROUP_ID = "addressConsumer";

    public static void main(String[] args) {
        Properties consumerProperties = Utils.createCustomConsumerProperties(
                GROUP_ID,
                Constants.BROKER,
                IntegerDeserializer.class.getName(),
                AddressDeserialiser.class.getName()
        );

        KafkaConsumer<Integer, Address> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Arrays.asList(UserDataProducer.TOPIC));

        while (true) {
            ConsumerRecords<Integer, Address> records = consumer.poll(1000);
            records.forEach(record -> {
                System.out.println("Key: " + record.key());
                System.out.println("Value: " + record.value() + "\n\n");
            });

        }
    }
}
