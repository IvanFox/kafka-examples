package me.ivanlis.avro.reflection;

import lombok.Data;
import org.apache.avro.reflect.Nullable;

@Data
public class CustomerData {

    private String name;

    private String city;

    @Nullable String company;

}
