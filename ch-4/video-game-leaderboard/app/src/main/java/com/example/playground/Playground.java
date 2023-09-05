package com.example.playground;

import com.sun.source.tree.Tree;

import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

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
    }
}
