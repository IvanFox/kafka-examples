package me.ivanlis.example.utils;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import me.ivanlis.example.bank.balance.serializers.TransactionSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

public class Utils {

    public static Properties createProducerProperties(String serverConfig) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", serverConfig);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "5");
        properties.setProperty("linger.ms", "1"); // flush message every ms
        return properties;
    }

    public static Properties createExactlyOnceProducer(String serverConfig, String keySerializer, String valueSerializer) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverConfig);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // must be set to all for indopotent
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1"); // flush message every ms
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return properties;
    }

    public static Properties createStreamProperties(String appId, String serverConfig) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, serverConfig);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    public static Properties createConsumerProperties(String groupId, String serverConfig) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", serverConfig);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000"); //every second the offset will be commited
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }



    public static void sleepFor(long seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
