package me.ivanlis.avro.producer;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.example.v2.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Properties;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class AvroProducer {

    public static final String TOPIC = "customer-avro";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ACKS_CONFIG, "1");
        properties.setProperty(RETRIES_CONFIG, "10");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:1122");

        KafkaProducer<String, Customer> producer = new KafkaProducer<>(properties);

        Customer customer = Customer.newBuilder()
                .setFirstName("John")
                .setLastName("Doe")
                .setAge(20)
                .setHeight(180.0f)
                .setWeight(74.3f).build();

        val record = new ProducerRecord<String, Customer>(TOPIC, customer);
        producer.send(record, (metadata, e) -> {
            if (e == null) {
                System.out.println("Success sent avro message!");
                System.out.println(metadata.toString());
            } else {
                System.out.println(e);
                System.out.println(metadata.toString());
            }

        });
        producer.flush();
        producer.close();

    }
}
