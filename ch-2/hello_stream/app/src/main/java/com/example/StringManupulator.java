package com.example;

@FunctionalInterface
public interface StringManupulator<T,R> {
    R apply(T t);
}
