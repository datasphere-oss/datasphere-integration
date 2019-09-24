package com.datasphere.utility;

import java.util.*;

public class GenerateTableFormat
{
    private static int PADDING_SIZE;
    private static String NEW_LINE;
    private static String TABLE_JOINT_SYMBOL;
    private static String TABLE_V_SPLIT_SYMBOL;
    private static String TABLE_H_SPLIT_SYMBOL;
    
    public static String generateTable(final List<String> headersList, final List<List<String>> rowsList, final int... overRiddenHeaderHeight) {
        final StringBuilder stringBuilder = new StringBuilder();
        final int rowHeight = (overRiddenHeaderHeight.length > 0) ? overRiddenHeaderHeight[0] : 1;
        final Map<Integer, Integer> columnMaxWidthMapping = getMaximumWidhtofTable(headersList, rowsList);
        stringBuilder.append(GenerateTableFormat.NEW_LINE);
        stringBuilder.append(GenerateTableFormat.NEW_LINE);
        createRowLine(stringBuilder, headersList.size(), columnMaxWidthMapping);
        stringBuilder.append(GenerateTableFormat.NEW_LINE);
        for (int headerIndex = 0; headerIndex < headersList.size(); ++headerIndex) {
            fillCell(stringBuilder, headersList.get(headerIndex), headerIndex, columnMaxWidthMapping);
        }
        stringBuilder.append(GenerateTableFormat.NEW_LINE);
        createRowLine(stringBuilder, headersList.size(), columnMaxWidthMapping);
        for (final List<String> row : rowsList) {
            for (int i = 0; i < rowHeight; ++i) {
                stringBuilder.append(GenerateTableFormat.NEW_LINE);
            }
            for (int cellIndex = 0; cellIndex < row.size(); ++cellIndex) {
                fillCell(stringBuilder, row.get(cellIndex), cellIndex, columnMaxWidthMapping);
            }
        }
        stringBuilder.append(GenerateTableFormat.NEW_LINE);
        createRowLine(stringBuilder, headersList.size(), columnMaxWidthMapping);
        stringBuilder.append(GenerateTableFormat.NEW_LINE);
        stringBuilder.append(GenerateTableFormat.NEW_LINE);
        return stringBuilder.toString();
    }
    
    private static void fillSpace(final StringBuilder stringBuilder, final int length) {
        for (int i = 0; i < length; ++i) {
            stringBuilder.append(" ");
        }
    }
    
    private static void createRowLine(final StringBuilder stringBuilder, final int headersListSize, final Map<Integer, Integer> columnMaxWidthMapping) {
        for (int i = 0; i < headersListSize; ++i) {
            if (i == 0) {
                stringBuilder.append(GenerateTableFormat.TABLE_JOINT_SYMBOL);
            }
            for (int j = 0; j < columnMaxWidthMapping.get(i) + GenerateTableFormat.PADDING_SIZE * 2; ++j) {
                stringBuilder.append(GenerateTableFormat.TABLE_H_SPLIT_SYMBOL);
            }
            stringBuilder.append(GenerateTableFormat.TABLE_JOINT_SYMBOL);
        }
    }
    
    private static Map<Integer, Integer> getMaximumWidhtofTable(final List<String> headersList, final List<List<String>> rowsList) {
        final Map<Integer, Integer> columnMaxWidthMapping = new HashMap<Integer, Integer>();
        for (int columnIndex = 0; columnIndex < headersList.size(); ++columnIndex) {
            columnMaxWidthMapping.put(columnIndex, 0);
        }
        for (int columnIndex = 0; columnIndex < headersList.size(); ++columnIndex) {
            if (headersList.get(columnIndex).length() > columnMaxWidthMapping.get(columnIndex)) {
                columnMaxWidthMapping.put(columnIndex, headersList.get(columnIndex).length());
            }
        }
        for (final List<String> row : rowsList) {
            for (int columnIndex2 = 0; columnIndex2 < row.size(); ++columnIndex2) {
                if (row.get(columnIndex2).length() > columnMaxWidthMapping.get(columnIndex2)) {
                    columnMaxWidthMapping.put(columnIndex2, row.get(columnIndex2).length());
                }
            }
        }
        for (int columnIndex = 0; columnIndex < headersList.size(); ++columnIndex) {
            if (columnMaxWidthMapping.get(columnIndex) % 2 != 0) {
                columnMaxWidthMapping.put(columnIndex, columnMaxWidthMapping.get(columnIndex) + 1);
            }
        }
        return columnMaxWidthMapping;
    }
    
    private static int getOptimumCellPadding(final int cellIndex, int datalength, final Map<Integer, Integer> columnMaxWidthMapping, int cellPaddingSize) {
        if (datalength % 2 != 0) {
            ++datalength;
        }
        if (datalength < columnMaxWidthMapping.get(cellIndex)) {
            cellPaddingSize += (columnMaxWidthMapping.get(cellIndex) - datalength) / 2;
        }
        return cellPaddingSize;
    }
    
    private static void fillCell(final StringBuilder stringBuilder, final String cell, final int cellIndex, final Map<Integer, Integer> columnMaxWidthMapping) {
        final int cellPaddingSize = getOptimumCellPadding(cellIndex, cell.length(), columnMaxWidthMapping, GenerateTableFormat.PADDING_SIZE);
        if (cellIndex == 0) {
            stringBuilder.append(GenerateTableFormat.TABLE_V_SPLIT_SYMBOL);
        }
        fillSpace(stringBuilder, cellPaddingSize);
        stringBuilder.append(cell);
        if (cell.length() % 2 != 0) {
            stringBuilder.append(" ");
        }
        fillSpace(stringBuilder, cellPaddingSize);
        stringBuilder.append(GenerateTableFormat.TABLE_V_SPLIT_SYMBOL);
    }
    
    static {
        GenerateTableFormat.PADDING_SIZE = 2;
        GenerateTableFormat.NEW_LINE = "\n";
        GenerateTableFormat.TABLE_JOINT_SYMBOL = "+";
        GenerateTableFormat.TABLE_V_SPLIT_SYMBOL = "|";
        GenerateTableFormat.TABLE_H_SPLIT_SYMBOL = "-";
    }
}
