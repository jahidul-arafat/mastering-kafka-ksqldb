package com.example.playground;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class MapVSFlatmap {
    public static void main(String[] args) {

        // Map Example
        List<Integer> originalIntList = Arrays.asList(1, 2, 3, 4, 5);
        List<Integer> modifiedIntList = originalIntList.stream()
                .filter(num -> num % 2 == 0)
                .map(num -> num * 2)
                .toList();
        System.out.printf("Modified List: %s\n", modifiedIntList); // [4, 8]

        // FlatMap Example
        List<List<Integer>> originalIntList2 = Arrays.asList(
                Arrays.asList(1, 2, 3, 4, 5),
                Arrays.asList(6, 7, 8, 9, 10),
                Arrays.asList(11, 12, 13, 14, 15)
        );
        System.out.println(originalIntList2); //[[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [11, 12, 13, 14, 15]]
        List<Integer> flatMappedIntList = originalIntList2.stream()
                .flatMap(Collection::stream)
                .toList();
        System.out.println(flatMappedIntList); // [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

        List<String> sentenceList = Arrays.asList("Hello World", "I am Ok", "I am fine", "Nice to meet you!");
        Stream<String> wordStream = sentenceList
                .stream()
                .flatMap(s -> Arrays.stream(s.split(" "))); // 1:N, when Map is 1:1
        System.out.println(wordStream.toList()); // [Hello, World, I, am, Ok, I, am, fine, Nice, to, meet, you!]


    }
}
