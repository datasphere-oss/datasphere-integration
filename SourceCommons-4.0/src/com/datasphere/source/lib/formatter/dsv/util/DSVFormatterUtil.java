package com.datasphere.source.lib.formatter.dsv.util;

import java.util.*;

import com.datasphere.source.lib.enums.*;
import com.datasphere.source.lib.prop.*;

import org.apache.commons.lang3.*;

public class DSVFormatterUtil
{
    private String columnDelimiter;
    private String rowDelimiter;
    private String nullValue;
    private boolean formatStrictly;
    private boolean useQuotes;
    private String quoteCharacter;
    private static final String COLUMN_DELIMITER = ",";
    private static final String QUOTE_CHARACTER = "\"";
    private static final String ROW_DELIMITER = "\r\n";
    private static final String CSV_RFC_STANDARD = "RFC4180";
    
    public DSVFormatterUtil(final Map<String, Object> formatterProperties) {
        this.formatStrictly = false;
        final Property prop = new Property(formatterProperties);
        final Object standard = prop.getObject("standard", Standard.none);
        final String standardName = standard.toString();
        if (standardName.equalsIgnoreCase("RFC4180")) {
            this.formatStrictly = true;
        }
        if (this.formatStrictly) {
            this.columnDelimiter = ",";
            this.quoteCharacter = "\"";
            this.rowDelimiter = "\r\n";
        }
        else {
            this.columnDelimiter = prop.getString("columndelimiter", ",");
            this.quoteCharacter = prop.getString("quotecharacter", "\"");
            this.rowDelimiter = prop.getString("rowdelimiter", "\n");
        }
        this.nullValue = prop.getString("nullvalue", "NULL");
        this.useQuotes = prop.getBoolean("usequotes", false);
    }
    
    public String quoteValue(String value) {
        if (this.formatStrictly) {
            value = StringEscapeUtils.escapeCsv(value);
            if (this.useQuotes && !value.contains("\"")) {
                value = this.quoteCharacter + value + this.quoteCharacter;
            }
        }
        else if (this.useQuotes) {
            if (value.contains(this.quoteCharacter)) {
                value = value.replaceAll(this.quoteCharacter, "\\\\" + this.quoteCharacter);
            }
            value = this.quoteCharacter + value + this.quoteCharacter;
        }
        return value;
    }
    
    public String getColumnDelimiter() {
        return this.columnDelimiter;
    }
    
    public String getRowDelimiter() {
        return this.rowDelimiter;
    }
    
    public String getQuoteCharacter() {
        return this.quoteCharacter;
    }
    
    public boolean isQuotesUsed() {
        return this.useQuotes;
    }
    
    public String getNullValue() {
        return this.nullValue;
    }
}
