package com.example.iot_sensor_data;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class BodyTempEventGenerator {
    public static void main(String[] args) throws IOException, ParseException {
        // Define the start timestamp
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date startTimestamp = dateFormat.parse("2020-11-23T09:03:06.500Z");

        // Create a JSON array to store the records
        JSONArray jsonArray = new JSONArray();

        // Generate patient data records for patients with IDs 1 to 3
        for (int patientId = 1; patientId <= 100; patientId++) {
            for (int i = 0; i < 500; i++) {
                long timestampMillis = startTimestamp.getTime() + (i * 500);
                Date timestamp = new Date(timestampMillis);

                JSONObject record = new JSONObject();
                record.put("timestamp", dateFormat.format(timestamp));
                record.put("temperature", generateRandomTemperature());
                record.put("unit", "F");
                jsonArray.put(patientId + "|" + record.toString());
            }
        }

        // Save the records to a JSON file
        try (FileWriter fileWriter = new FileWriter("body-temp-events-gen.json")) {
            for (int i = 0; i < jsonArray.length(); i++) {
                fileWriter.write(jsonArray.getString(i) + "\n");
            }
        }
    }

    private static double generateRandomTemperature() {
        // Generate a random temperature between 97.0°F and 104.0°F
        return Math.round((97.0 + (Math.random() * 7.0)) * 10.0) / 10.0;
    }
}
