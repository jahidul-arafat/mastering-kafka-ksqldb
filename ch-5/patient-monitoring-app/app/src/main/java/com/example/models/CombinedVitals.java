package com.example.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data // getter, setter and toString
@AllArgsConstructor
public class CombinedVitals {
    private final int heartRate;
    private final BodyTemp bodyTemp;
}
