package edu.touro.las.mcon364.streams.ds;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

public class WeatherDataScienceExercise {

    record WeatherRecord(
        String stationId,
        String city,
        String date,
        double temperatureC,
        int humidity,
        double precipitationMm
    ) {}

    public static void main(String[] args) throws Exception {
        List<String> rows = Files.readAllLines(Path.of("noaa_weather_sample.csv"));

        List<WeatherRecord> cleaned = rows.stream()
            .skip(1)
            .map(WeatherDataScienceExercise::parseRow)
            .flatMap(Optional::stream)
            .filter(WeatherDataScienceExercise::isValid)
            .toList();

        System.out.println("Cleaned data:");
        cleaned.forEach(System.out::println);

        double avgTemp = cleaned.stream()
            .mapToDouble(WeatherRecord::temperatureC)
            .average()
            .orElse(0);

        System.out.println("\nAverage temperature: " + avgTemp);

        Map<String, Double> avgByCity = cleaned.stream()
            .collect(Collectors.groupingBy(
                WeatherRecord::city,
                Collectors.averagingDouble(WeatherRecord::temperatureC)
            ));

        System.out.println("\nAverage temp by city:");
        avgByCity.forEach((c, v) -> System.out.println(c + ": " + v));
    }

    static Optional<WeatherRecord> parseRow(String row) {
        String[] parts = row.split(",");
        if (parts.length != 6) return Optional.empty();

        try {
            if (parts[3].isBlank()) return Optional.empty();

            return Optional.of(new WeatherRecord(
                parts[0],
                parts[1],
                parts[2],
                Double.parseDouble(parts[3]),
                Integer.parseInt(parts[4]),
                Double.parseDouble(parts[5])
            ));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    static boolean isValid(WeatherRecord r) {
        return r.temperatureC() >= -60 && r.temperatureC() <= 60
            && r.humidity() >= 0 && r.humidity() <= 100
            && r.precipitationMm() >= 0;
    }
}
