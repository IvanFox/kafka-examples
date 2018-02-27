package me.ivanlis.example.bank.balance;

import com.google.gson.Gson;
import java.util.Properties;
import me.ivanlis.example.bank.balance.messages.Transaction;
import me.ivanlis.example.utils.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

public class TotalBalanceAggregator {

    private final static String APP_ID = "transaction_aggregator";

    private final static Gson GSON = new Gson();

    public final static String TOTAL_BALANCE_TOPIC = "totalBalance";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.RESET_OFFSET_EARLIEST);


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> transactions = builder.stream(TransactionProducer.TRANSACTION_TOPIC);

        KTable<String, Long> totalBalance = transactions.groupByKey()
                .aggregate(
                        () -> 0L,
                        ((key, value, aggregate) -> aggregate + GSON.fromJson(value, Transaction.class).getAmount().longValue()),
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(
                                "total_count" /* table/store name */)
                                .withKeySerde(Serdes.String()) /* key serde */
                                .withValueSerde(Serdes.Long()) /* value serde */
                );

        totalBalance.to(Serdes.String(), Serdes.Long(), TOTAL_BALANCE_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // print topology
        System.out.println(streams.localThreadsMetadata());

        // close app gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
