package me.ivanlis.avro.evolution;

import java.io.File;
import java.io.IOException;
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

        CustomerV1 customerV1 = CustomerV1.newBuilder()
                .setFirstName("Ivan")
                .setLastName("Fox")
                .setWeight(75f)
                .setHeight(178.8f)
                .setAge(30)
                .build();

        System.out.println("Customer version 1: " + customerV1);

        final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>(CustomerV1.class);
        final DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<>(datumWriter);

        try {
            dataFileWriter.create(customerV1.getSchema(), new File("customer_v1.avro"));
            dataFileWriter.append(customerV1);
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Persist customer v1 to disc");

        System.out.println("Reading customer v1 with customer v2 schema");


        final DatumReader<CustomerV2> datumReader = new SpecificDatumReader<>(CustomerV2.class);

        try {
            final DataFileReader<CustomerV2> dataFileReader = new DataFileReader<CustomerV2>(new File("customer_v1.avro"), datumReader);
            while (dataFileReader.hasNext()) {
                CustomerV2 customerV2 = dataFileReader.next();
                System.out.println("Customer v2: " + customerV2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
