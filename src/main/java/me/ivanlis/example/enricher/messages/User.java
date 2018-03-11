package me.ivanlis.example.enricher.messages;

import lombok.Value;

@Value
public class User {

    Integer userId;

    String name;

    Address address;

    PhoneNumber phoneNumber;

}
