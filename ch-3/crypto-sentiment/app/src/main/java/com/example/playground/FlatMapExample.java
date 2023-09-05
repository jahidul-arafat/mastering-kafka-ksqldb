package com.example.playground;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlatMapExample {
    public static void main(String[] args) {
        List<String> sentenceList = Arrays.asList("Hello World", "I am Ok", "I am fine", "Nice to meet you!");
        var wordStream = sentenceList
                .stream()
                .flatMap(s -> Arrays.stream(s.split(" "))); // 1:N, when Map is 1:1


        // Calculate word frequencies (word:count)
        Map<String, Long> wordFrequencyMap = wordStream
                .collect(Collectors.groupingBy(
                        String::toLowerCase,
                        Collectors.counting()
                ));

        wordFrequencyMap.forEach((word, count) -> System.out.println(word + ":" + count));

        // flatMap vs Map
        // map() function produces one output for one input value, whereas flatMap() function produces an arbitrary no of values as output

        List<String> numList = Arrays.asList("1 12 11111", "2", "3", "4", "5");
        // using flatmap
        numList.stream()
                .flatMap(num -> Stream.of(num.split(" ")))
                        .forEach(System.out::println);

        // using map
        numList.stream()
                .map(num -> Stream.of(num.split(" ")))
                .forEach(System.out::println);

    }
}
