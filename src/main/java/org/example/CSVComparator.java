package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CSVComparator {

    public static void main(String[] args) throws Exception {

        List<Map<String, String>> csvData1 = readCSV("SampleData1.csv");
        List<Map<String, String>> csvData2 = readCSV("SampleData2.csv");


        Map<String, String> file1ColumnTypes = determineColumnTypes(csvData1);
        Map<String, String> file2ColumnTypes = determineColumnTypes(csvData2);


        System.out.println("File 1 Column Types: " + file1ColumnTypes);
        System.out.println("File 2 Column Types: " + file2ColumnTypes);


        compareColumns(file1ColumnTypes, file2ColumnTypes, csvData1, csvData2);
    }

    private static List<Map<String, String>> readCSV(String filePath) throws Exception {
        List<Map<String, String>> records = new ArrayList<>();
        try (Reader in = new FileReader(filePath)) {
            Iterable<CSVRecord> recordsIterable = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .parse(in);
            for (CSVRecord record : recordsIterable) {
                Map<String, String> recordMap = new HashMap<>();
                for (String column : record.toMap().keySet()) {
                    recordMap.put(column, record.get(column));
                }
                records.add(recordMap);
            }
        }
        return records;
    }

    private static Map<String, String> determineColumnTypes(List<Map<String, String>> csvData) {
        Map<String, String> columnTypes = new HashMap<>();
        if (csvData.isEmpty()) return columnTypes;

        Map<String, String> firstRecord = csvData.get(0);
        for (String column : firstRecord.keySet()) {
            String type = determineTypeForColumn(csvData, column);
            columnTypes.put(column, type);
        }
        return columnTypes;
    }


    private static String determineTypeForColumn(List<Map<String, String>> csvData, String column) {
        boolean isInt = true, isDouble = true, isDate = true, isPhoneNumber = true;
        for (Map<String, String> record : csvData) {
            String value = record.get(column);
            if (value == null || value.trim().isEmpty()) continue;

            value = value.trim();

            if (isInt) {
                try {
                    Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    isInt = false;
                }
            }

            if (isDouble) {
                try {
                    Double.parseDouble(value.replace(",", ""));
                } catch (NumberFormatException e) {
                    isDouble = false;
                }
            }

            if (isDate) {
                isDate = isValidDate(value);
            }

            if (isPhoneNumber) {
                isPhoneNumber = value.matches("\\d{10}");
            }

            if (!isInt && !isDouble && !isDate && !isPhoneNumber) break;
        }

        if (isPhoneNumber) return "PhoneNumber";
        if (isInt) return "Integer";
        if (isDouble) return "Double";
        if (isDate) return "Date";
        return "String";
    }



    private static boolean isValidDate(String value) {
        List<String> dateFormats = Arrays.asList("MM/dd/yyyy", "dd/MM/yyyy", "yyyy-MM-dd");
        for (String format : dateFormats) {
            if (isDateFormat(value, format)) {
                return true;
            }
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


    private static void compareColumns(Map<String, String> file1ColumnTypes, Map<String, String> file2ColumnTypes,
                                       List<Map<String, String>> csvData1, List<Map<String, String>> csvData2) {
        System.out.println("Comparing columns between File 1 and File 2:");

        for (String column1 : file1ColumnTypes.keySet()) {
            String type1 = file1ColumnTypes.get(column1);

            for (String column2 : file2ColumnTypes.keySet()) {
                String type2 = file2ColumnTypes.get(column2);


                if (type1.equals(type2)) {

                    if (areColumnsSimilar(csvData1, csvData2, column1, column2)) {
                        System.out.println("Columns '" + column1 + "' and '" + column2 + "' have similar data.");
                    }
                }
            }
        }
    }



    private static boolean areColumnsSimilar(List<Map<String, String>> csvData1, List<Map<String, String>> csvData2, String column1, String column2) {
        Set<String> column1Values = new HashSet<>();
        Set<String> column2Values = new HashSet<>();

        for (Map<String, String> record : csvData1) {
            String value = record.get(column1);
            if (value != null && !value.trim().isEmpty()) {
                column1Values.add(value.trim());
            }
        }

        for (Map<String, String> record : csvData2) {
            String value = record.get(column2);
            if (value != null && !value.trim().isEmpty()) {
                column2Values.add(value.trim());
            }
        }


        Set<String> commonValues = new HashSet<>(column1Values);
        commonValues.retainAll(column2Values);


        double thresholdPercentage = 50.0;


        int smallerSetSize = Math.min(column1Values.size(), column2Values.size());
        double overlapPercentage = ((double) commonValues.size() / smallerSetSize) * 100;


        return overlapPercentage >= thresholdPercentage;
    }

}
