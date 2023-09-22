package com.example.iot_sensor_data;

import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class PulseEventGeneratorOptimized {
    public static void main(String[] args) throws IOException, ParseException {
        // Define the start timestamp
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date startTimestamp = dateFormat.parse("2020-11-23T09:02:00.000Z");

        // Create a FileWriter to write records to a file
        try (FileWriter fileWriter = new FileWriter("pulse-events-gen.json");
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {

            // Generate pulse records for patients with IDs 1 to 10000
            for (int patientId = 1; patientId <= 110; patientId++) {
                for (int i = 0; i < 50000; i++) {
                    long timestampMillis = startTimestamp.getTime() + (i * 500);
                    Date timestamp = new Date(timestampMillis);

                    JSONObject record = new JSONObject();
                    record.put("patientID", patientId);
                    record.put("timestamp", dateFormat.format(timestamp));

                    // Write the record to the file
                    bufferedWriter.write(patientId + "|" + record.toString() + "\n");
                }
            }
        }
    }
}
