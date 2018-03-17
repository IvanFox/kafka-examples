package me.ivanlis.streaming.example.favourite_color_app;

import static java.util.Collections.singletonList;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class TotalColorCounterConsumer {
    private final static String GROUP_ID = "totalColorCounterC";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", LongDeserializer.class.getName());
        properties.setProperty("group.id", GROUP_ID);
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000"); //every second the offset will be commited
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, Long> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(singletonList(TotalColorCounter.TOTAL_FAV_COLOR_COUNTER_TOPIC));

        while (true) {
            ConsumerRecords<String, Long> records = kafkaConsumer.poll(1000);
            records.forEach(record -> {
                System.out.println("Key:" + record.key());
                System.out.println("Value:" + record.value());
                System.out.println();
            });

        }
    }
}
