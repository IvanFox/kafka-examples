package me.ivanlis.streaming.example.favourite_color_app;

import static me.ivanlis.streaming.example.utils.Utils.createProducerProperties;
import static me.ivanlis.streaming.example.utils.Utils.sleepFor;

import java.util.Arrays;
import java.util.List;
import me.ivanlis.streaming.example.favourite_color_app.FavColor.Color;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ColorProducer {

    public static final String FAV_COLOR_INPUT_TOPIC = "fav_color_input";

    private static final String STEPHEN = "Stephen";
    private static final String IVAN = "Ivan";
    private static final String EVELINA = "Evelina";
    private static final String KARINA = "Karina";
    private static final String MICHAEL = "Michael";


    public static void main(String[] args) {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(createProducerProperties("localhost:9092"));

        List<FavColor> msgs = Arrays.asList(
                new FavColor(IVAN, Color.BLACK),
                new FavColor(KARINA, Color.RED)
//                new FavColor(MICHAEL, Color.RED),
//                new FavColor(EVELINA, Color.BLUE),
//                new FavColor(STEPHEN, Color.BLUE)
        );

        msgs.forEach(msg -> {
            sleepFor(30);
            System.out.println("Sending " + msg + " to kafka.");
            kafkaProducer.send(new ProducerRecord<>(FAV_COLOR_INPUT_TOPIC, msg.getName(), msg.getColor().toString()));
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
