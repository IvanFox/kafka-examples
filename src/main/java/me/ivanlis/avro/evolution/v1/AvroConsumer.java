package me.ivanlis.avro.evolution.v1;

import static java.util.Arrays.asList;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Properties;
import me.ivanlis.Customer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class AvroConsumer {

    private final static String GROUP_ID = "avro-consumer";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:1122");
        properties.setProperty("specific.avro.reader", "true");

        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(asList(AvroProducer.TOPIC));

        while(true) {
            ConsumerRecords<String, Customer> records = consumer.poll(100L);
            records.forEach(record -> System.out.println(record.value()));
            consumer.commitAsync();
        }
//        consumer.close();
    }

}
