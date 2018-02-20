package me.ivanlis.example.favourite_color_app;

import java.util.Properties;
import me.ivanlis.example.utils.Utils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

public class ColorCounter {

    public static final String CURRENT_FAV_COLOR_COUNT_OUTPUT_TOPIC = "current_fav_color";

    private static final String APP_ID = "favColorAppTest";

    public static void main(String[] args) {
        Properties properties = Utils.createStreamProperties(APP_ID, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> favColorInput = builder.stream(ColorProducer.FAV_COLOR_INPUT_TOPIC);

        KTable<String, String> currentFavColor = favColorInput.groupByKey()
                .reduce((oldVal, newValue ) -> newValue, Materialized.as("Count"));

        currentFavColor.to(Serdes.String(), Serdes.String(), CURRENT_FAV_COLOR_COUNT_OUTPUT_TOPIC);


        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // print topology
        System.out.println(streams.localThreadsMetadata());

        // close app gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
