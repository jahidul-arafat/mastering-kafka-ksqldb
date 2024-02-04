package com.example;

import org.apache.kafka.streams.Topology;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.example.App.FILENAME;
import static com.example.App.TOPOLOGY_DESC;

// Common Stream Processing Logic for both DSL and Processing API
public class StreamTransformationLogic {
    public static void commonBusinessProcessingLogic(String key, String content, String appInstanceName) {
        // System.out.println("Applying the Business Logic to the Stream input for Stream Transformation by the Stream Processor");
        // Business Logic for the transformation
        // create a function to transform the value of the record into upper case
        StringManupulator<String, String> stringManupulator = (demoString) -> demoString.toUpperCase(); // FunctionalInterface //Atomic
        Function<String, Integer> lenCalFunc = (demoString) -> demoString.length(); // Function
        Function<String, String> isTooShort = (demoString) -> {
            return lenCalFunc.apply(demoString) > 10 ? "Full Length" : "too Short"; //lambda function
        };

        // check if the key is NULL or not; if NULL, then replace it with a default value
        key = key == null ? "NULL" : key;    // lambda checking if key is NULL or not

        // define the predicate to check if vowel
        Predicate<Character> isVowel = c -> "aeiou".indexOf(Character.toLowerCase(c)) != -1; // Predicate to check if the character is vowel; -1 means character not found in the vowel list
        // count the vowels using this predicate
        Function<String, Long> countVowels = (demoString) -> demoString.chars()
                .mapToObj(ch -> (char) ch)// converting the IntStream <the ASCII value of a character> into Stream<Character>
                .filter(isVowel)
                .count();

        String formattedString = String.format("(%s) <Key: %s> Value<%s>. Transformation<Name Length: %d(%s), Vowel Count is: %d>",
                appInstanceName,
                key, // key from the input stream
                stringManupulator.apply(content), // a functional interface // example value -> JAHIDULARAFAT
                lenCalFunc.apply(content),    // // 8
                isTooShort.apply(content),   // Yes
                countVowels.apply(content));  // i.e 3 vowels
        System.out.println(formattedString);

        // Writing the transformed/ enhanced stream into a file
        String formattedStringForAppendOnly = String.format("[%s] %s -> %s,%d,%s,%d",
                appInstanceName,
                key,
                stringManupulator.apply(content),
                lenCalFunc.apply(content),
                isTooShort.apply(content),
                countVowels.apply(content));
        appendToFile(FILENAME, formattedStringForAppendOnly);
    }

    // Part of STREAM PROCESSOR
    // method to write a string into a text file into append only mode
    public static void appendToFile(String filename, String content) {
        String msgToConsole = String.format("Content: <%s> .... writing to file >> <%s>\n", content,filename);
        System.out.printf(msgToConsole);

        // write the content into the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {
            writer.write(content + "\n");
        } catch (IOException e) {
            e.printStackTrace();
            // Handle the exception as needed (e.g., log it or throw it)
        }
    }

    // redundant method to write a List of Strings into a text file into append only mode
    public static void addListContentToFile(String filename, @NotNull List<String> content) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {
            for (String s : content) {
                writer.write(s + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
            // Handle the exception as needed (e.g., log it or throw it)
        }
    }

    public static void topologyDescriptor(@NotNull Topology topology) {
        // describe the topology and write it to a file
        try (FileWriter fw = new FileWriter(TOPOLOGY_DESC)) {
            fw.write(topology.describe().toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
