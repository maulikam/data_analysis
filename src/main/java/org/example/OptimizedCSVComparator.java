package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class OptimizedCSVComparator {

    private static final int CHUNK_SIZE = 10000;
    private static final double SIMILARITY_THRESHOLD = 0.5;

    public static void main(String[] args) throws Exception {
        String file1 = "SampleData1.csv";
        String file2 = "SampleData2.csv";

        Map<String, String> file1ColumnTypes = determineColumnTypes(file1);
        Map<String, String> file2ColumnTypes = determineColumnTypes(file2);

        System.out.println("File 1 Column Types: " + file1ColumnTypes);
        System.out.println("File 2 Column Types: " + file2ColumnTypes);

        compareColumns(file1, file2, file1ColumnTypes, file2ColumnTypes);
    }

    private static Map<String, String> determineColumnTypes(String filePath) throws IOException {
        Map<String, String> columnTypes = new HashMap<>();
        try (Reader reader = new FileReader(filePath);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            Map<String, Integer> header = parser.getHeaderMap();
            Iterator<CSVRecord> iterator = parser.iterator();

            if (iterator.hasNext()) {
                CSVRecord firstRecord = iterator.next();
                for (String column : header.keySet()) {
                    String type = determineType(firstRecord.get(column));
                    columnTypes.put(column, type);
                }
            }
        }
        return columnTypes;
    }

    private static String determineType(String value) {
        if (value == null || value.trim().isEmpty()) return "String";
        value = value.trim();

        if (value.matches("\\d{10}")) return "PhoneNumber";
        if (value.matches("-?\\d+")) return "Integer";
        if (value.matches("-?\\d*\\.?\\d+")) return "Double";
        if (isValidDate(value)) return "Date";
        return "String";
    }

    private static boolean isValidDate(String value) {
        List<String> dateFormats = Arrays.asList("MM/dd/yyyy", "dd/MM/yyyy", "yyyy-MM-dd");
        for (String format : dateFormats) {
            if (isDateFormat(value, format)) return true;
        }
        return false;
    }

    private static boolean isDateFormat(String value, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setLenient(false);
        try {
            sdf.parse(value);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    private static void compareColumns(String file1, String file2,
                                       Map<String, String> file1ColumnTypes,
                                       Map<String, String> file2ColumnTypes) throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<ColumnSimilarity>> futures = new ArrayList<>();

        for (Map.Entry<String, String> entry1 : file1ColumnTypes.entrySet()) {
            for (Map.Entry<String, String> entry2 : file2ColumnTypes.entrySet()) {
                if (entry1.getValue().equals(entry2.getValue())) {
                    futures.add(executor.submit(() -> compareColumnData(file1, file2, entry1.getKey(), entry2.getKey())));
                }
            }
        }

        for (Future<ColumnSimilarity> future : futures) {
            try {
                ColumnSimilarity similarity = future.get();
                if (similarity.getSimilarity() >= SIMILARITY_THRESHOLD) {
                    System.out.println("Columns '" + similarity.getColumn1() + "' and '" +
                            similarity.getColumn2() + "' have similar data. Similarity: " +
                            String.format("%.2f", similarity.getSimilarity()));
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();
    }

    private static ColumnSimilarity compareColumnData(String file1, String file2, String column1, String column2) throws IOException {
        Set<String> values1 = new HashSet<>();
        Set<String> values2 = new HashSet<>();

        readColumnValues(file1, column1, values1);
        readColumnValues(file2, column2, values2);

        Set<String> commonValues = new HashSet<>(values1);
        commonValues.retainAll(values2);

        int smallerSetSize = Math.min(values1.size(), values2.size());
        double similarity = (double) commonValues.size() / smallerSetSize;

        return new ColumnSimilarity(column1, column2, similarity);
    }

    private static void readColumnValues(String filePath, String columnName, Set<String> values) throws IOException {
        try (Reader reader = new FileReader(filePath);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            int columnIndex = parser.getHeaderMap().get(columnName);
            parser.stream()
                    .map(record -> record.get(columnIndex))
                    .filter(value -> value != null && !value.trim().isEmpty())
                    .forEach(value -> values.add(value.trim()));
        }
    }

    private static class ColumnSimilarity {
        private final String column1;
        private final String column2;
        private final double similarity;

        public ColumnSimilarity(String column1, String column2, double similarity) {
            this.column1 = column1;
            this.column2 = column2;
            this.similarity = similarity;
        }

        public String getColumn1() { return column1; }
        public String getColumn2() { return column2; }
        public double getSimilarity() { return similarity; }
    }
}