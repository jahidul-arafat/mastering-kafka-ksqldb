package com.example.models;

public enum Type {
    DESIRED("desired"),
    REPORTED("reported");

    private final String reported;

    Type(String reported) {
        this.reported = reported;
    }
}
