package me.ivanlis.streaming.example.enricher.producers;

import java.util.Arrays;
import java.util.Properties;
import me.ivanlis.streaming.example.enricher.messages.User;
import me.ivanlis.streaming.example.utils.Constants;
import me.ivanlis.streaming.example.utils.Utils;
import me.ivanlis.streaming.example.enricher.serialisers.UserSerde.UserSerialiser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class UserDataProducer {

    public static final Integer MAX_USER_ID = 5;

    private static final User[] USERS = {
            new User(0,"James Doe"),
            new User(1,"James Rodriguez"),
            new User(2,"Sanctioned man"),
            new User(3,"Good Guy"),
            new User(4,"Adam Smith"),
            new User(MAX_USER_ID,"Karina Star")
    };

    public static final String TOPIC = "user_info";



    public static void main(String[] args) throws InterruptedException {
        Properties producerProperties = Utils.createExactlyOnceProducer(
                Constants.BROKER,
                IntegerSerializer.class.getName(),
                UserSerialiser.class.getName()
        );

        KafkaProducer<Integer, User> kafkaProducer = new KafkaProducer<>(producerProperties);

        Arrays.asList(USERS).forEach(user -> {
            Utils.sleepFor(2);
            kafkaProducer.send(new ProducerRecord<>(TOPIC, user.getUserId(), user));
        });
    }




}
