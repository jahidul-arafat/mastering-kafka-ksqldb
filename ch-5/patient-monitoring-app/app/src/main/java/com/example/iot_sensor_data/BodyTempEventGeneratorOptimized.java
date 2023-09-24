package com.example.iot_sensor_data;

import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class BodyTempEventGeneratorOptimized {
    public static void main(String[] args) throws IOException, ParseException {
        // Define the start timestamp
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date startTimestamp = dateFormat.parse("2020-11-23T09:03:06.500Z");

        // Create a FileWriter to write records to a file
        try (FileWriter fileWriter = new FileWriter("body-temp-events-gen.json");
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {

            // Generate patient data records for patients with IDs 1 to 10000
            for (int patientId = 1; patientId <= 3; patientId++) {
                for (int i = 0; i < 200; i++) {
                    long timestampMillis = startTimestamp.getTime() + (i * 500);
                    Date timestamp = new Date(timestampMillis);

                    JSONObject record = new JSONObject();
                    record.put("timestamp", dateFormat.format(timestamp));
                    record.put("temperature", generateRandomTemperature());
                    record.put("unit", "F");

                    // Write the record to the file
                    bufferedWriter.write(patientId + "|" + record.toString() + "\n");
                }
            }
        }

    }

    private static double generateRandomTemperature() {
        // Generate a random temperature between 97.0°F and 104.0°F
        return Math.round((97.0 + (Math.random() * 7.0)) * 10.0) / 10.0;
    }
}
