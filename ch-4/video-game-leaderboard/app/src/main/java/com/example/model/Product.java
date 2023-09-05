package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// 1|{"id": 1, "name": "Super Smash Bros"}
// For Kafka Topic: "products"
// Goes to Deserializer and Serializer
@Data
@AllArgsConstructor
@NoArgsConstructor  // Default constructor
public class Product {
    private Long id;
    private String name;
}
