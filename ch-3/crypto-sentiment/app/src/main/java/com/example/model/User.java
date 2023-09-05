package com.example.model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor // default constructor
public class User {
    private String id;

    private String name;

//    private String description;
//    private String screenName;
//    private String url;
//    private String followersCount;
//    private String friendsCount;
}
