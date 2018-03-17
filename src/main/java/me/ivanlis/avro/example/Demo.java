package me.ivanlis.avro.example;

import com.example.v2.Customer;
import java.util.Arrays;
import me.ivanlis.v2.CustomerAddress;
import me.ivanlis.v2.Type;

public class Demo {

    public static void main(String[] args) {
        Customer customer = Customer.newBuilder()
                .setFirstName("Ivan")
                .setLastName("Fox")
                .setAge(31)
                .setWeight(74.5f)
                .setHeight(180.0f)
                .setCustomerEmails(Arrays.asList())
                .setCustomerAddress(new CustomerAddress("", "", "", Type.ENTERPRISE))
//                .setAutomatedEmail(false)
                .build();

        System.out.println(customer);


    }
}
