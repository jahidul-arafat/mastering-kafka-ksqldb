package com.example.playground;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlatMapExample {
    public static void main(String[] args) {
        List<String> sentenceList = Arrays.asList("Hello World", "I am Ok", "I am fine", "Nice to meet you!");
        List<String> wordStream = sentenceList
                .stream()
                .flatMap(s -> Arrays.stream(s.split(" "))) // 1:N, when Map is 1:1
                .toList(); // earlier code has error her; you cant left a stream silo, instead returned it as a List which will be consumed by downstream processors

        // print the stream wordStream
        //System.out.printf("Printing the stream [FlatMap applied]: ");
        System.out.println(wordStream);
        wordStream.forEach(System.out::println); // stream is consumed here; A Stream can only be consumed once


        // Calculate word frequencies (word:count)
        Map<Object, Long> wordFrequencyMap = wordStream.stream()
                .collect(Collectors.groupingBy(
                        String::toLowerCase,
                        Collectors.counting()
                ));

        System.out.println(wordFrequencyMap);
        wordFrequencyMap.forEach((word, count) -> System.out.println(word + ":" + count));

        // flatMap vs Map
        // map() function produces one output for one input value, whereas flatMap() function produces an arbitrary no of values as output

        List<String> numList = Arrays.asList("1 12 11111", "2", "3", "4", "5");
        // using flatmap
        numList.stream()
                .flatMap(num -> Stream.of(num.split(" ")))
                .forEach(System.out::println);

        // using map
//        numList.stream()
//                .map(num -> Stream.of(num.split(" ")))
//                .forEach(System.out::println);

    }
}
