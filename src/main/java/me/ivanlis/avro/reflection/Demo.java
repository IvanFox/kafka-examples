package me.ivanlis.avro.reflection;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

public class Demo {

    public static void main(String[] args) {
        Schema schema1 = ReflectData.get().getSchema(Customer.class);

//        System.out.println("Schema: " + schema1.toString(true));
//
//        System.out.println("");
        Schema schema2 = ReflectData.get().getSchema(CustomerData.class);
        System.out.println("Schema: " + schema2.toString(true));

    }
}
