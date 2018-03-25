package me.ivanlis.avro.evolution;

import static java.util.Collections.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import me.ivanlis.CustomerV1;
import me.ivanlis.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class SchemaEvolutionDemo {

    public static void main(String[] args) {
        final String location = "customer_v1.avro";
        final CustomerV1 customerV1 = createCustomerV1();
        System.out.println("Customer version 1: " + customerV1);

        System.out.println("Persisting customer v1 to a disk");
        persistCustomerV1(customerV1, location);

        System.out.println("Reading customer v1 with customer v2 schema");
        List<CustomerV2> customerV2s = readCustomerV2(location);
        System.out.println(customerV2s);

    }

    private static CustomerV1 createCustomerV1(){
        return CustomerV1.newBuilder()
                .setFirstName("Ivan")
                .setLastName("Fox")
                .setWeight(75f)
                .setHeight(178.8f)
                .setAge(30)
                .build();
    }

    private static void persistCustomerV1(CustomerV1 customerV1, String location) {
        final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>(CustomerV1.class);
        final DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<>(datumWriter);

        try {
            dataFileWriter.create(customerV1.getSchema(), new File("customer_v1.avro"));
            dataFileWriter.append(customerV1);
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<CustomerV2> readCustomerV2(String location) {
        final DatumReader<CustomerV2> datumReader = new SpecificDatumReader<>(CustomerV2.class);
        List<CustomerV2> customersV2 = new ArrayList<>();
        try {
            final DataFileReader<CustomerV2> dataFileReader = new DataFileReader<>(new File(location), datumReader);
            while (dataFileReader.hasNext()) {
                customersV2.add(dataFileReader.next());
            }
            return customersV2;
        } catch (IOException e) {
            return emptyList();
        }
    }
}
