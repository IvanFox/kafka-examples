package me.ivanlis.streaming.example.enricher.producers;

import static me.ivanlis.streaming.example.utils.Utils.createExactlyOnceProducer;
import static me.ivanlis.streaming.example.utils.Utils.sleepFor;

import java.util.Properties;
import java.util.Random;
import me.ivanlis.streaming.example.enricher.messages.PhoneNumber;
import me.ivanlis.streaming.example.enricher.serialisers.PhoneNumberSerde;
import me.ivanlis.streaming.example.utils.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class PhoneNumberProducer {

    private static final PhoneNumber PHONE_NUMBERS[] = {
            new PhoneNumber("123-456-789"),
            new PhoneNumber("234-567-890"),
            new PhoneNumber("345-678-901"),
            new PhoneNumber("456-789-012"),
            new PhoneNumber("567-890-123"),
    };

    private static final Random RANDOM_GENERATOR = new Random();

    public static final String TOPIC = "phone_number_change";


    public static void main(String[] args) {
        Properties producerProperties = createExactlyOnceProducer(
                Constants.BROKER,
                IntegerSerializer.class.getName(),
                PhoneNumberSerde.PhoneNumberSerialiser.class.getName()
        );

        KafkaProducer<Integer, PhoneNumber> producer = new KafkaProducer<>(producerProperties);

        while(true) {
            producer.send(new ProducerRecord<>(
                    TOPIC,
                    RANDOM_GENERATOR.nextInt(UserDataProducer.MAX_USER_ID),
                    PHONE_NUMBERS[RANDOM_GENERATOR.nextInt(PHONE_NUMBERS.length)]
            ));
            sleepFor(3);
        }


    }
}
