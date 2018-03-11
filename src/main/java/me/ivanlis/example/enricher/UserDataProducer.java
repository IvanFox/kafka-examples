package me.ivanlis.example.enricher;

import java.util.Arrays;
import java.util.Properties;
import me.ivanlis.example.enricher.messages.User;
import me.ivanlis.example.enricher.serialisers.UserSerde;
import me.ivanlis.example.utils.Constants;
import me.ivanlis.example.utils.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class UserDataProducer {

    public static final Integer MAX_USER_ID = 5;

    private static final User[] USERS = {
            new User(1,"James Doe", null, null),
            new User(2,"James Rodriguez", null, null),
            new User(2,"Sanctioned man", null, null),
            new User(3,"Good Guy", null, null),
            new User(4,"Adam Smith", null, null),
            new User(MAX_USER_ID,"Karina Star", null, null)
    };

    public static final String USER_TOPIC = "user_info";



    public static void main(String[] args) throws InterruptedException {
        Properties producerProperties = Utils.createExactlyOnceProducer(
                Constants.BROKER,
                IntegerSerializer.class.getName(),
                UserSerde.UserSerialiser.class.getName()
        );

        KafkaProducer<Integer, User> kafkaProducer = new KafkaProducer<>(producerProperties);

        Arrays.asList(USERS).forEach(user -> {
            Utils.sleepFor(2);
            kafkaProducer.send(new ProducerRecord<>(USER_TOPIC, user.getUserId(), user));
        });
    }




}
