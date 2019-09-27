package com.datasphere.source.csv;

import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.constant.Constant;
import com.datasphere.source.lib.meta.Column;
import com.datasphere.source.lib.meta.StringColumn;
import com.datasphere.source.lib.rs.CharResultSet;
import com.datasphere.source.lib.rs.MetaData;

public class CSVResultSetMetaData extends MetaData
{
    private String nextFileName;
    private String prevFileName;
    CSVProperty prop;
    CharResultSet resultSet;
    Hashtable<Integer, Column> columnMetaData;
    int columnCount;
    public boolean isValid;
    static Logger logger;
    
    public CSVResultSetMetaData(final CharResultSet waResultSet) throws IOException, InterruptedException {
        this.nextFileName = null;
        this.prevFileName = null;
        this.columnCount = 0;
        this.isValid = false;
        this.columnCount = waResultSet.getColumnCount();
        if (this.columnCount > 0) {
            this.isValid = true;
            this.columnMetaData = new Hashtable<Integer, Column>(this.columnCount);
            for (int i = 0; i < this.columnCount; ++i) {
                final Column column = new StringColumn();
                column.setIndex(i);
                column.setName(waResultSet.getColumnValue(i));
                column.setType(Constant.fieldType.STRING);
                this.columnMetaData.put(i, column);
            }
        }
    }
    
    public CSVResultSetMetaData(final CharResultSet waResultSet, final CSVProperty csvprop) throws IOException, InterruptedException, AdapterException {
        this.nextFileName = null;
        this.prevFileName = null;
        this.columnCount = 0;
        this.isValid = false;
        this.prop = csvprop;
        this.resultSet = waResultSet;
        if (this.resultSet.reader().name() != null) {
            if (CSVResultSetMetaData.logger.isDebugEnabled()) {
                CSVResultSetMetaData.logger.debug((Object)("If header isn't present in the property no action is taken on" + this.getCurrentFileName()));
            }
            if (!this.prop.header) {
                if (this.prop.headerlineno == 0) {
                    return;
                }
            }
            try {
                if (csvprop.positionByEOF) {
                    this.resultSet.positionToBeginning();
                }
                final com.datasphere.common.constants.Constant.recordstatus rs = this.resultSet.next();
                if (rs == com.datasphere.common.constants.Constant.recordstatus.VALID_RECORD) {
                    this.isValid = true;
                }
            }
            catch (IOException e) {
                CSVResultSetMetaData.logger.error((Object)e);
            }
            this.columnCount = this.resultSet.getColumnCount();
            this.columnMetaData = new Hashtable<Integer, Column>(this.columnCount);
            for (int i = 0; i < this.columnCount; ++i) {
                final Column column = new StringColumn();
                column.setIndex(i);
                column.setName(this.resultSet.getColumnValue(i));
                column.setType(Constant.fieldType.STRING);
                this.columnMetaData.put(i, column);
            }
            if (csvprop.positionByEOF) {
                this.resultSet.reset();
            }
        }
    }
    
    public List<String> getInterestedMetadata() {
        final String metadataRegex = this.prop.metaColumnList;
        final Pattern pattern = Pattern.compile(metadataRegex, 2);
        final List<String> interestedMetadataList = new LinkedList<String>();
        for (int index = 0; index < this.columnCount; ++index) {
            final String value = this.getColumnName(index);
            final Matcher patternMatcher = pattern.matcher(value);
            while (patternMatcher.find()) {
                interestedMetadataList.add(patternMatcher.group());
            }
        }
        return interestedMetadataList;
    }
    
    public String getCurrentFileName() {
        return this.resultSet.reader().name();
    }
    
    public String getNextFileName() {
        return this.nextFileName;
    }
    
    public String getPrevFileName() {
        return this.prevFileName;
    }
    
    public int getColumnCount() {
        return this.columnMetaData.size();
    }
    
    public String getColumnName(final int index) {
        if (!this.columnMetaData.isEmpty()) {
            return this.columnMetaData.get(index).getName();
        }
        return null;
    }
    
    public Constant.fieldType getColumnType(final int index) {
        if (!this.columnMetaData.isEmpty()) {
            return this.columnMetaData.get(index).getType();
        }
        return Constant.fieldType.STRING;
    }
    
    static {
        CSVResultSetMetaData.logger = Logger.getLogger((Class)CSVResultSetMetaData.class);
    }
}
