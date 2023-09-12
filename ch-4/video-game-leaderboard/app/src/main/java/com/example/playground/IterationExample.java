package com.example.playground;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class IterationExample {
    public static void main(String[] args) {
        Map<Integer, String> nameMap = new HashMap<>();
        nameMap.put(1, "John");
        nameMap.put(2, "Mary");
        nameMap.put(3, "Peter");

        // get an iterator for the map's entry set
        Iterator<Map.Entry<Integer, String>> entrySetIterator = nameMap.entrySet().iterator();

        // iterate over the entry set
        while (entrySetIterator.hasNext()) {
            var entry = entrySetIterator.next();
            Integer key = entry.getKey();
            String value = entry.getValue();
            System.out.println(key + " " + value);

        }
    }
}
