package me.ivanlis.example.bank.balance.messages;

import java.math.BigDecimal;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
public class Balance {

    private final String name;

    private final BigDecimal balance;

    private final int count;

    public Balance(String name, BigDecimal balance, Integer count) {
        this.name = name;
        this.balance = balance;
        this.count = count;
    }

    public Balance(String name, Balance balance, Transaction transaction) {
        this.name = name;
        this.balance = balance.balance.add(transaction.getAmount());
        this.count = (balance.getCount() + 1);
    }
}
