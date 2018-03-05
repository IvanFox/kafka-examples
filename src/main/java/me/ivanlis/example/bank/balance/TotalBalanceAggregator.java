package me.ivanlis.example.bank.balance;

import java.math.BigDecimal;
import java.util.Properties;
import me.ivanlis.example.bank.balance.messages.Balance;
import me.ivanlis.example.bank.balance.messages.Transaction;
import me.ivanlis.example.bank.balance.serializers.BalanceDeserializer;
import me.ivanlis.example.bank.balance.serializers.BalanceSerializer;
import me.ivanlis.example.bank.balance.serializers.TransactionDeserializer;
import me.ivanlis.example.bank.balance.serializers.TransactionSerde;
import me.ivanlis.example.bank.balance.serializers.TransactionSerializer;
import me.ivanlis.example.utils.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;

public class TotalBalanceAggregator {

    private final static String APP_ID = "transaction_aggregator1";

    public final static String TOTAL_BALANCE_TOPIC = "totalBalance";

    public static void main(String[] args) {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TransactionSerde.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.RESET_OFFSET_EARLIEST);

        // Exactly once guarantee
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        final Serde<Balance> balanceSerde = Serdes.serdeFrom(new BalanceSerializer(), new BalanceDeserializer());

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Transaction> transactions = builder.stream(TransactionProducer.TRANSACTION_TOPIC);

        final KTable<String, Balance> totalBalance = transactions
                .groupByKey()
                .aggregate(
                        () -> new Balance("", new BigDecimal(0)),
                        ((key, value, aggregate) -> Balance.calculateBalance(aggregate, value)),
                        Materialized.<String, Balance, KeyValueStore<Bytes, byte[]>>as(
                                "total_count" /* table/store name */)
                                .withKeySerde(Serdes.String()) /* key serde */
                                .withValueSerde(balanceSerde) /* value serde */
                );

        totalBalance.to(Serdes.String(), balanceSerde, TOTAL_BALANCE_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // print topology
        System.out.println(streams.localThreadsMetadata());

        // close app gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
