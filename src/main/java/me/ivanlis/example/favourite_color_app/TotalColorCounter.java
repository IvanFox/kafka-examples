package me.ivanlis.example.favourite_color_app;

import static me.ivanlis.example.utils.Utils.createStreamProperties;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

public class TotalColorCounter {

    public static final String TOTAL_FAV_COLOR_COUNTER_TOPIC = "total_fav_color_counter";

    private static final String APP_ID = "totalColorCounterApp";

    public static void main(String[] args) {
        Properties properties = createStreamProperties(APP_ID, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> favColorOutput = builder.stream(ColorCounter.CURRENT_FAV_COLOR_COUNT_OUTPUT_TOPIC);

        KTable<String, Long> count = favColorOutput
                .selectKey(((key, value) -> value))
                .groupByKey()
                .count(Materialized.as("ColorCount"));


        count.to(Serdes.String(), Serdes.Long(), TOTAL_FAV_COLOR_COUNTER_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
        // print topology
        System.out.println(streams.localThreadsMetadata());

        // close app gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }


}
