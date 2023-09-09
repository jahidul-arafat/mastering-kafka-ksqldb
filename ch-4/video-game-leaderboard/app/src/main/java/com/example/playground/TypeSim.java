package com.example.playground;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;

public class TypeSim {
    public static void main(String[] args) {
        // Use GSON to deserialize a JSON arrray into a List of 'Person' objects using the specified 'Type'
        // Object: To demonstrate how 'Type' is used to provide type information for deserialziation

        // 1. Create a GSON instance
        Gson gson = new Gson();

        // 2. Define a JSON String what we will be deserializing to object 'Person'
        // Person class has no attribute 'gender', but this deserialization still would work
        String jsonString = "[{\"name\":\"John\",\"age\":30, \"gender\":\"M\"},{\"name\":\"Mary\",\"age\":29}]";

        // 3. Create a Type Object that represents a List of Person objects
        Type personListType_predicateToCaptureTypeInfo = new TypeToken<List<Person>>() {}.getType(); // from gson
                                                // .getType() -> to get the Type information of the captured information

        // 4. Deserialize the JSON String into a List of Person objects
        List<Person> deserializedPersonList = gson.fromJson(
                jsonString,
                personListType_predicateToCaptureTypeInfo);

        // print the list of Person objects
        System.out.println("Deserialized Person");
        deserializedPersonList.forEach(System.out::println);

    }




}
