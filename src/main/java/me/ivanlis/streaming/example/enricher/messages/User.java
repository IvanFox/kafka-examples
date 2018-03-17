package me.ivanlis.streaming.example.enricher.messages;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class User {

    private final Integer userId;

    private final String name;

    private final Address address;

    private final PhoneNumber phoneNumber;

    public User(Integer userId, String name, Address address) {
        this.userId = userId;
        this.name = name;
        this.address = address;
        this.phoneNumber = new PhoneNumber("empty");
    }

    public User(Integer userId, String name) {
        this.userId = userId;
        this.name = name;
        this.address = new Address("empty");
        this.phoneNumber = new PhoneNumber("empty");

    }

    public User(Integer userId, String name, Address address, PhoneNumber phoneNumber) {
        this.userId = userId;
        this.name = name;

        this.address = address;
        this.phoneNumber = phoneNumber;
    }
}
