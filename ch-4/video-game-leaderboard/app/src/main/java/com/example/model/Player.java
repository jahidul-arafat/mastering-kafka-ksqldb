package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// 1|{"id": 1, "name": "Elyse"}
// For Kafka Topic: "players"
// Goes to Deserializer and Serializer
@Data
@AllArgsConstructor
@NoArgsConstructor // default constructor
public class Player {
    private Long id;
    private String name;
}
