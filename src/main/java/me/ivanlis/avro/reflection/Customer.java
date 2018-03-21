package me.ivanlis.avro.reflection;

import org.apache.avro.reflect.Nullable;

public class Customer {

    private String name;
    private String location;
    @Nullable private String company;


    public Customer() { }

    public Customer(String name, String location, String company) {
        this.name = name;
        this.location = location;
        this.company = company;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }
}
