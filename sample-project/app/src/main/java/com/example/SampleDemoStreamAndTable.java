package com.example;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SampleDemoStreamAndTable {
    public static void main(String[] args) {
        // create a List of Map entry with Key as String and Value as Integer
        var demoStream = List.of(
                Map.entry("a", 1),
                Map.entry("b", 1),
                Map.entry("a", 2)
        );
        // create a new hash map
        Map<String,Integer> table = new HashMap<>();
        demoStream.forEach(entry -> table.put(entry.getKey(), entry.getValue()));

        // print the stream
        System.out.println(demoStream); //[a=1, b=1, a=2]

        // print the table
        System.out.println(table); //{a=2, b=1}

        // convert the table into demoStream
        List<Map.Entry<String,Integer>> demoStreamFromTable = table
                .entrySet()
                .stream()
                .collect(Collectors.toList());
        // add an element in the demo stream
        demoStreamFromTable.add(Map.entry("c", 3));
        // print the demo stream
        System.out.println(demoStreamFromTable); // [a=2, b=1, c=3]

    }
}
