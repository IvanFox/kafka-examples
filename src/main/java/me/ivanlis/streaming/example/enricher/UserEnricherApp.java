package me.ivanlis.streaming.example.enricher;

import static org.apache.kafka.common.serialization.Serdes.Integer;

import java.util.Properties;
import me.ivanlis.streaming.example.enricher.messages.Address;
import me.ivanlis.streaming.example.enricher.messages.PhoneNumber;
import me.ivanlis.streaming.example.enricher.messages.User;
import me.ivanlis.streaming.example.enricher.producers.AddressDataProducer;
import me.ivanlis.streaming.example.enricher.producers.PhoneNumberProducer;
import me.ivanlis.streaming.example.enricher.producers.UserDataProducer;
import me.ivanlis.streaming.example.enricher.serialisers.AddressSerde;
import me.ivanlis.streaming.example.enricher.serialisers.UserSerde;
import me.ivanlis.streaming.example.utils.Constants;
import me.ivanlis.streaming.example.enricher.serialisers.PhoneNumberSerde;
import me.ivanlis.streaming.example.utils.Utils;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;

public class UserEnricherApp {

    private static final String APP_ID = "enricherApp0";
    private static final boolean EXACTLY_ONCE = true;

    public static final String USER_ADDRESS_JOIN = "user_address_join";
    public static final String USER_ADDRESS_PHONE_JOIN = "user_address_phone_join";

    public static void main(String[] args) {
        Properties properties = Utils.createCustomStreamProperties(
                APP_ID,
                Constants.BROKER,
                Integer().getClass(),
                UserSerde.userSerde().getClass(),
                EXACTLY_ONCE
        );

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<Integer, User> usersGlobalTable = builder.globalTable(UserDataProducer.TOPIC);

        KStream<Integer, Address> addressStream = builder.stream(
                AddressDataProducer.TOPIC, Consumed.with(Integer(), AddressSerde.addressSerde()));

        KStream<Integer, User> userAddressJoin = addressStream
                .join(usersGlobalTable,
                        ((key, value) -> key),
                        (address, user) -> new User(user.getUserId(), user.getName(), address))
                .peek(((key, value) -> System.out.println(value)));

        userAddressJoin.to(USER_ADDRESS_JOIN);

        KStream<Integer, User> aggregatedUser = builder.stream(USER_ADDRESS_JOIN, Consumed.with(Integer(), UserSerde.userSerde()));

        KStream<Integer, PhoneNumber> phoneStream = builder.stream(
                PhoneNumberProducer.TOPIC, Consumed.with(Integer(), PhoneNumberSerde.phoneNumberSerde()));

        KStream<Integer, User> fullJoin = aggregatedUser
                .leftJoin(phoneStream,
                        (user, phone) -> new User(user.getUserId(), user.getName(), user.getAddress(), phone),
                        JoinWindows.of(1000), Joined.with(Integer(), UserSerde.userSerde(), PhoneNumberSerde.phoneNumberSerde()))
                .peek(((key, value) -> System.out.println(value)));


        fullJoin.to(USER_ADDRESS_PHONE_JOIN);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // print topology
        System.out.println(streams.localThreadsMetadata());

        // close app gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
