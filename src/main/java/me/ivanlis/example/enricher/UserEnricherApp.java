package me.ivanlis.example.enricher;

import static me.ivanlis.example.utils.Utils.createCustomStreamProperties;

import java.util.Properties;
import me.ivanlis.example.enricher.messages.Address;
import me.ivanlis.example.enricher.messages.PhoneNumber;
import me.ivanlis.example.enricher.messages.User;
import me.ivanlis.example.enricher.producers.AddressDataProducer;
import me.ivanlis.example.enricher.producers.UserDataProducer;
import me.ivanlis.example.enricher.serialisers.AddressSerde;
import me.ivanlis.example.enricher.serialisers.UserSerde;
import me.ivanlis.example.utils.Constants;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.IntegerSerde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

public class UserEnricherApp {

    private static final String APP_ID = "enricherApp2";
    private static final boolean EXACTLY_ONCE = true;

    public static final String USER_ADDRESS_JOIN = "user_address_join";

    public static void main(String[] args) {
        Properties properties = createCustomStreamProperties(
                APP_ID,
                Constants.BROKER,
                Serdes.Integer().getClass(),
                UserSerde.userSerde().getClass(),
                EXACTLY_ONCE
        );

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<Integer, User> usersGlobalTable = builder.globalTable(UserDataProducer.TOPIC);

        KStream<Integer, Address> addressStream = builder
                .stream(AddressDataProducer.TOPIC, Consumed.with(Serdes.Integer(), AddressSerde.addressSerde()));

        KStream<Integer, User> userAddressJoin = addressStream
                .join(usersGlobalTable,
                        ((key, value) -> key),
                        (address, user) -> new User(user.getUserId(), user.getName(), address))
                .peek(((key, value) -> System.out.println(value)));

        userAddressJoin.to(USER_ADDRESS_JOIN);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // print topology
        System.out.println(streams.localThreadsMetadata());

        // close app gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
