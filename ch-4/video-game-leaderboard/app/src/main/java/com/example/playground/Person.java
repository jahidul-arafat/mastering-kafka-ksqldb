package com.example.playground;

import com.example.model.stateful_join_models.EnrichedWithAll;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person implements Comparable<Person>{
    private String name;
    private int age;

    // implement the comparable interface
    @Override
    public int compareTo(Person o) {
        return Integer.compare(this.age, o.age);
    }
}
