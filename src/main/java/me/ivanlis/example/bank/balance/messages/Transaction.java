package me.ivanlis.example.bank.balance.messages;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
public class Transaction {

    String name;

    BigDecimal amount;

    LocalDateTime localDateTime;


    public Transaction(String name, BigDecimal amount) {
        this.name = name;
        this.amount = amount;
        this.localDateTime = LocalDateTime.now();
    }
}
