package com.example.playground;

import org.apache.kafka.common.protocol.types.Field;

import java.util.*;
import java.util.stream.Collectors;

// Note Flatmap is Lazy
/*

Output Simulation:
The Structure before flattening is : [[5, 7, 11, 13], [1, 3, 5], [2, 4, 6, 8]]
The Structure after flattening is : [5, 7, 11, 13, 1, 3, 5, 2, 4, 6, 8]
The Structure after mapping is : [4, 16, 36, 64]
Before Flatting: [{1=a, 11=bb}, {2=b}, {1=c}]
After Flatting: [1=a, 11=bb, 2=b, 1=c]
 */
public class FlatMapExample {
    public static void main(String[] args) {
        // Creating a list of Prime Numbers
        List<Integer> PrimeNumbers = Arrays.asList(5, 7, 11,13);

        // Creating a list of Odd Numbers
        List<Integer> OddNumbers = Arrays.asList(1, 3, 5);

        // Creating a list of Even Numbers
        List<Integer> EvenNumbers = Arrays.asList(2, 4, 6, 8);

        List<List<Integer>> listOfListofInts =
                Arrays.asList(PrimeNumbers, OddNumbers, EvenNumbers);

        System.out.println("The Structure before flattening is : " +
                listOfListofInts);

        // Using flatMap for transformating and flattening.
        List<Integer> listofInts  = listOfListofInts.stream()
                .flatMap(list -> list.stream())
                .collect(Collectors.toList());

        System.out.println("The Structure after flattening is : " +
                listofInts);

        // using flatMap for transformation and return values which are divisible by 2 or 4 and map those to be squared
        List<Integer> listofInts1  = listOfListofInts.stream()
              .flatMap(list -> list.stream())
              .filter(i -> i % 2 == 0 || i % 4 == 0)
              .map(i -> i * i)
              .collect(Collectors.toList());

        System.out.println("The Structure after mapping is : " +
                listofInts1);

        List<Map<Integer, String>> records = new ArrayList<>();
        var element1= new HashMap<Integer, String>();
        element1.put(1, "a");
        element1.put(11,"aa");
        element1.put(11,"bb");

        var element2= new HashMap<Integer, String>();
        element2.put(2, "b");

        var element3= new HashMap<Integer, String>();
        element3.put(1, "c");
        records.add(element1);
        records.add(element2);
        records.add(element3);

        System.out.println("Before Flatting: "+records);

        // Flatten the list of maps and print the elements
        List<Map.Entry<Integer, String>> flattenedList = records.stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toList());

        // Print the flattened list elements
        System.out.println("After Flatting: "+flattenedList);


    }
}
