package me.ivanlis.example.bank.balance.messages;

import java.math.BigDecimal;
import lombok.Value;

@Value
public class Balance {

    String name;

    BigDecimal balance;

    public static Balance calculateBalance(Balance oldBalance, Transaction transaction) {
        return new Balance(oldBalance.name, oldBalance.balance.add(transaction.getAmount()));
    }
}
