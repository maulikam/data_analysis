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

public class CsvColumnComparator {

    private static final int CHUNK_SIZE = 100000;
    private static final double SIMILARITY_THRESHOLD = 0.5;

    private static final Pattern INTEGER_PATTERN = Pattern.compile("-?\\d+");
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("-?\\d*\\.?\\d+");
    private static final Pattern PHONE_PATTERN = Pattern.compile("\\d{10}");

    public static void main(String[] args) throws Exception {
        String firstFilePath = "SampleData1.csv";
        String secondFilePath = "SampleData2.csv";

        Map<String, String> firstFileColumnTypes = determineColumnTypes(firstFilePath);
        Map<String, String> secondFileColumnTypes = determineColumnTypes(secondFilePath);

        System.out.println("File 1 Column Types: " + firstFileColumnTypes);
        System.out.println("File 2 Column Types: " + secondFileColumnTypes);

        compareColumns(firstFilePath, secondFilePath, firstFileColumnTypes, secondFileColumnTypes);
    }

    private static Map<String, String> determineColumnTypes(String filePath) throws IOException {
        Map<String, String> columnTypes = new HashMap<>();
        try (Reader fileReader = new FileReader(filePath);
             CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(fileReader)) {

            Map<String, Integer> header = csvParser.getHeaderMap();
            Iterator<CSVRecord> recordIterator = csvParser.iterator();

            if (recordIterator.hasNext()) {
                CSVRecord firstRecord = recordIterator.next();
                for (String columnName : header.keySet()) {
                    String columnType = inferColumnType(firstRecord.get(columnName));
                    columnTypes.put(columnName, columnType);
                }
            }
        }
        return columnTypes;
    }

    private static String inferColumnType(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "String";
        }
        value = value.trim();

        if (PHONE_PATTERN.matcher(value).matches()) {
            return "PhoneNumber";
        }
        if (INTEGER_PATTERN.matcher(value).matches()) {
            return "Integer";
        }
        if (DECIMAL_PATTERN.matcher(value).matches()) {
            return "Decimal";
        }
        if (isValidDate(value)) {
            return "Date";
        }
        return "String";
    }

    private static boolean isValidDate(String value) {
        List<String> dateFormats = Arrays.asList("MM/dd/yyyy", "dd/MM/yyyy", "yyyy-MM-dd");
        for (String format : dateFormats) {
            if (isValidDateFormat(value, format)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isValidDateFormat(String value, String format) {
        try {
            new SimpleDateFormat(format).parse(value);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    private static void compareColumns(String firstFilePath, String secondFilePath,
                                       Map<String, String> firstFileColumnTypes,
                                       Map<String, String> secondFileColumnTypes) throws IOException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<ColumnSimilarity>> similarityFutures = new ArrayList<>();

        for (Map.Entry<String, String> firstFileColumn : firstFileColumnTypes.entrySet()) {
            for (Map.Entry<String, String> secondFileColumn : secondFileColumnTypes.entrySet()) {
                if (firstFileColumn.getValue().equals(secondFileColumn.getValue())) {
                    similarityFutures.add(executorService.submit(() -> compareColumnData(
                            firstFilePath, secondFilePath, firstFileColumn.getKey(), secondFileColumn.getKey())));
                }
            }
        }

        for (Future<ColumnSimilarity> future : similarityFutures) {
            try {
                ColumnSimilarity columnSimilarity = future.get();
                if (columnSimilarity.getSimilarity() >= SIMILARITY_THRESHOLD) {
                    System.out.printf("Columns '%s' (File: %s) and '%s' (File: %s) have similar data. Similarity: %.2f%n",
                            columnSimilarity.getFirstColumn(), columnSimilarity.getFirstFilePath(),
                            columnSimilarity.getSecondColumn(), columnSimilarity.getSecondFilePath(),
                            columnSimilarity.getSimilarity());
                }
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }

    private static ColumnSimilarity compareColumnData(String firstFilePath, String secondFilePath,
                                                      String firstColumn, String secondColumn) throws IOException {
        Set<String> firstFileValues = ConcurrentHashMap.newKeySet();
        Set<String> secondFileValues = ConcurrentHashMap.newKeySet();

        readColumnValues(firstFilePath, firstColumn, firstFileValues);
        readColumnValues(secondFilePath, secondColumn, secondFileValues);

        return calculateColumnSimilarity(firstFilePath, secondFilePath, firstFileValues, secondFileValues, firstColumn, secondColumn);
    }

    private static void readColumnValues(String filePath, String columnName, Set<String> values) throws IOException {
        try (Reader fileReader = new FileReader(filePath);
             CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(fileReader)) {

            int columnIndex = csvParser.getHeaderMap().get(columnName);
            for (CSVRecord record : csvParser) {
                String value = record.get(columnIndex);
                if (value != null && !value.trim().isEmpty()) {
                    values.add(value.trim());
                }
            }
        }
    }

    private static ColumnSimilarity calculateColumnSimilarity(String firstFilePath, String secondFilePath,
                                                              Set<String> firstValues, Set<String> secondValues,
                                                              String firstColumn, String secondColumn) {
        Set<String> commonValues = new HashSet<>(firstValues);
        commonValues.retainAll(secondValues);

        int smallerSetSize = Math.min(firstValues.size(), secondValues.size());
        double similarity = (double) commonValues.size() / smallerSetSize;

        return new ColumnSimilarity(firstFilePath, secondFilePath, firstColumn, secondColumn, similarity);
    }

    private static class ColumnSimilarity {
        private final String firstFilePath;
        private final String secondFilePath;
        private final String firstColumn;
        private final String secondColumn;
        private final double similarity;

        public ColumnSimilarity(String firstFilePath, String secondFilePath, String firstColumn, String secondColumn, double similarity) {
            this.firstFilePath = firstFilePath;
            this.secondFilePath = secondFilePath;
            this.firstColumn = firstColumn;
            this.secondColumn = secondColumn;
            this.similarity = similarity;
        }

        public String getFirstFilePath() {
            return firstFilePath;
        }

        public String getSecondFilePath() {
            return secondFilePath;
        }

        public String getFirstColumn() {
            return firstColumn;
        }

        public String getSecondColumn() {
            return secondColumn;
        }

        public double getSimilarity() {
            return similarity;
        }
    }
}
