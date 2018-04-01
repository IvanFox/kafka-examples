package me.ivanlis.avro.evolution.v1;

import static me.ivanlis.avro.evolution.AvroProducerHelper.createAvroProducer;

import lombok.val;
import me.ivanlis.Customer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AvroProducer {

    public static final String TOPIC = "customer-avro";

    public static void main(String[] args) {
        KafkaProducer<String, Customer> producer = createAvroProducer(Customer.class, "localhost:9092", "http://localhost:1122");

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
