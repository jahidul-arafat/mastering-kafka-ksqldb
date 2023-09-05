package com.example.playground;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TestMethod {
    private TreeSet<String> childList;

    public void add(String child) {
        this.childList.add(child);
    }

    public List<String> toList() {
        return childList.stream().toList();
    }
}
