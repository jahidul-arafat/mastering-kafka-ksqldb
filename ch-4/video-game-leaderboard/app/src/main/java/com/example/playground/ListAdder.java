package com.example.playground;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
public class ListAdder {
    private final List<String> strList = new ArrayList<>();

    // method to add a string to the list
    public ListAdder add(String str) {
        strList.add(str);
        return this;
    }

}
