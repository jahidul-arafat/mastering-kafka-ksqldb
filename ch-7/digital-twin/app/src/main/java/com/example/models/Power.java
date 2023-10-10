package com.example.models;

public enum Power {
    ON("on"),
    OFF("off"),
    ;

    private final String status; //ON/OFF, restrictive modification, only value is assigned, cant be modified

    Power(String status) {
        this.status = status;
    }
}
