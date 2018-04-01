package me.ivanlis.avro.example;

import java.io.File;
import java.io.IOException;
import me.ivanlis.Customer;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class Demo {

    public static void main(String[] args) throws IOException {
        Customer customer = Customer.newBuilder()
                .setFirstName("Ivan")
                .setLastName("Fox")
                .setAge(31)
                .setWeight(74.5f)
                .setHeight(180.0f)
//                .setCustomerEmails(Arrays.asList())
//                .setCustomerAddress(new CustomerAddress("", "", "", Type.ENTERPRISE))
                .setAutomatedEmail(false)
                .build();


        System.out.println(customer);


        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(customer.getSchema(), new File("users.avro"));
        dataFileWriter.append(customer);
        dataFileWriter.close();

    }
}
