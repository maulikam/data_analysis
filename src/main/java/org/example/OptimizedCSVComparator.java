package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

public class OptimizedCSVComparator {

    private static final int CHUNK_SIZE = 100000;
    private static final double SIMILARITY_THRESHOLD = 0.5;

    private static final Pattern INTEGER_PATTERN = Pattern.compile("-?\\d+");
    private static final Pattern DOUBLE_PATTERN = Pattern.compile("-?\\d*\\.?\\d+");
    private static final Pattern PHONE_PATTERN = Pattern.compile("\\d{10}");

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

        if (PHONE_PATTERN.matcher(value).matches()) return "PhoneNumber";
        if (INTEGER_PATTERN.matcher(value).matches()) return "Integer";
        if (DOUBLE_PATTERN.matcher(value).matches()) return "Double";
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
        try {
            new SimpleDateFormat(format).parse(value);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    private static void compareColumns(String file1, String file2,
                                       Map<String, String> file1ColumnTypes,
                                       Map<String, String> file2ColumnTypes) throws IOException, InterruptedException {
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
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
    }

    private static ColumnSimilarity compareColumnData(String file1, String file2, String column1, String column2) throws IOException {
        Set<String> values1 = ConcurrentHashMap.newKeySet();
        Set<String> values2 = ConcurrentHashMap.newKeySet();

        readColumnValues(file1, column1, values1);
        readColumnValues(file2, column2, values2);

        return calculateSimilarity(values1, values2, column1, column2);
    }

    private static void readColumnValues(String filePath, String columnName, Set<String> values) throws IOException {
        try (Reader reader = new FileReader(filePath);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            int columnIndex = parser.getHeaderMap().get(columnName);
            for (CSVRecord record : parser) {
                String value = record.get(columnIndex);
                if (value != null && !value.trim().isEmpty()) {
                    values.add(value.trim());
                }
            }
        }
    }

    private static ColumnSimilarity calculateSimilarity(Set<String> values1, Set<String> values2, String column1, String column2) {
        Set<String> commonValues = new HashSet<>(values1);
        commonValues.retainAll(values2);

        int smallerSetSize = Math.min(values1.size(), values2.size());
        double similarity = (double) commonValues.size() / smallerSetSize;

        return new ColumnSimilarity(column1, column2, similarity);
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

        public String getColumn1() {
            return column1;
        }

        public String getColumn2() {
            return column2;
        }

        public double getSimilarity() {
            return similarity;
        }
    }
}