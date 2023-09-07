package com.example.playground;

import com.sun.source.tree.Tree;

import java.util.*;
import java.util.stream.Collectors;

public class Playground {
    public static void main(String[] args) {
        // create a TreeSet
        TreeSet<String> children = new TreeSet<>(); // The shorted set // HashSet or LinkedHashSet are not shorted
        children.add("Alice");
        children.add("Bob");
        children.add("Billy");
        children.add("Cilly");

        List<String> childrenList = children
                .stream().toList();
        System.out.println(childrenList);

        TestMethod testMethod = new TestMethod(children);
        testMethod.add("Jahid");
        testMethod.add("Testy");
        System.out.println(testMethod.getChildList());
        System.out.println(testMethod.toList());

        // a list of integers
        List<Integer> numList = Arrays.asList(1, 2, 3, 4, 5, 6,7);
        System.out.println(numList);
        var mappedNumList = numList.stream()
                .map(item->item*2)
                .toList();
        System.out.println(mappedNumList);

        // reduce the list
        var reducedNumListToTotal = mappedNumList
                .stream()
                        .reduce(0, (a, b) -> a + b);
        System.out.println(reducedNumListToTotal);

        // count the number of elements in the list
        var count = mappedNumList
               .stream()
                       .count();
        System.out.println(count);

        // create a Map of Strings to Integers
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "Alice");
        map.put(2, "Bob");
        map.put(3, "Billy");
        map.put(4, "Alice");

        // print the map
        map.forEach((key, value) -> System.out.println(key + " : " + value));

        // group the elements of the map by the value
        // Group the map by values (names)
        Map<String, List<Integer>> groupedMapByName = map.entrySet().stream()
                .collect(Collectors.groupingBy(Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

        Map<String,Integer> summedMap = new HashMap<>();

        groupedMapByName.forEach((name, keys) -> {
                    int sum = keys
                            .stream()
                            .reduce(0, (a, b) -> a + b);
                    summedMap.put(name, sum);
                });
        System.out.println("Group-Sum-Total-Reduce");
        groupedMapByName.forEach((key, value) ->System.out.println(key + " -> " + value));
        summedMap.forEach((key, value) -> System.out.println(key + "::"+ value));
        System.out.println();

        // Group the elements of the map by value with counting
        Map<String, Long> groupedMapByNameCount = map.entrySet().stream()
               .collect(Collectors.groupingBy(Map.Entry::getValue,
                        Collectors.counting()));
        groupedMapByNameCount.forEach((key, value) ->System.out.println(key + " : " + value));




    }
}
