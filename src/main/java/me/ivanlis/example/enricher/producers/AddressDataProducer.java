package me.ivanlis.example.enricher.producers;

import static me.ivanlis.example.utils.Utils.createExactlyOnceProducer;
import static me.ivanlis.example.utils.Utils.sleepFor;

import java.util.Properties;
import java.util.Random;
import me.ivanlis.example.enricher.messages.Address;
import me.ivanlis.example.enricher.serialisers.AddressSerde;
import me.ivanlis.example.utils.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class AddressDataProducer {

    private static final Address[] ADDRESSES = {
            new Address("Narva"),
            new Address("Tallinn"),
            new Address("Tartu"),
            new Address("Parnu"),
            new Address("Rakvere"),
            new Address("Kohla-Jarve")
    };

    public static final String TOPIC = "address_change";

    private static final Random RANDOM_GENERATOR = new Random();


    public static void main(String[] args) {
        Properties producerProperties = createExactlyOnceProducer(
                Constants.BROKER,
                IntegerSerializer.class.getName(),
                AddressSerde.AddressSerialiser.class.getName()
        );

        KafkaProducer<Integer, Address> kafkaProducer = new KafkaProducer<>(producerProperties);

        while (true) {
            kafkaProducer.send(new ProducerRecord<>(
                    TOPIC,
                    RANDOM_GENERATOR.nextInt(UserDataProducer.MAX_USER_ID),
                    ADDRESSES[RANDOM_GENERATOR.nextInt(ADDRESSES.length)]
            ));
            sleepFor(3);
        }
    }
}
