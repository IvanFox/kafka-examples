package me.ivanlis.example.favourite_color_app;

import static java.util.Collections.singletonList;
import static me.ivanlis.example.utils.Utils.createDefaultConsumerProperties;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ColorInputConsumer {

    private final static String GROUP_ID = "fav_color_consumer_test";

    public static void main(String[] args) {
        Properties consumerProperties = createDefaultConsumerProperties(GROUP_ID, "localhost:9092");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);

        kafkaConsumer.subscribe(singletonList(ColorProducer.FAV_COLOR_INPUT_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            records.forEach(record -> {
                System.out.println("Key:" + record.key());
                System.out.println("Value:" + record.value());
                System.out.println();
            });

        }
    }

}
