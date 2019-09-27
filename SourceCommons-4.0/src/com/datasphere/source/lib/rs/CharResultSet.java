package com.datasphere.source.lib.rs;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.common.constants.Constant;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.source.nvp.NVPProperty;
import com.datasphere.source.nvp.NameValueParser;

public abstract class CharResultSet extends ResultSet
{
    protected char[] rowData;
    Logger logger;
    
    public CharResultSet(final Reader reader, final Property prop) throws IOException, InterruptedException {
        super(reader, prop);
        this.logger = Logger.getLogger((Class)ResultSet.class);
    }
    
    @Override
    protected void Init() throws IOException, InterruptedException {
        this.rowData = new char[this.reader.blockSize()];
        super.Init();
    }
    
    public String getColumnValue(final int columnIndex) {
        if (this.columnLength[columnIndex] == 0 || this.columnOffset[columnIndex] == -1) {
            return "";
        }
        final String value = new String(this.rowData, this.columnOffset[columnIndex], this.columnLength[columnIndex]);
        if (this.prop.trimwhitespace) {
            return value.trim();
        }
        return value;
    }
    
    public char[] getRowData() {
        return this.rowData;
    }
    
    public void setRowData(final char[] rowData) {
        this.rowData = rowData;
    }
    
    @Override
    public Constant.recordstatus next() throws IOException, InterruptedException {
        return null;
    }
    
    @Override
    public Map<String, String> getColumnValueAsMap(final int index) throws AdapterException {
        return this.getColumnValueAsMap(this.getColumnValue(index));
    }
    
    public Map<String, String> getColumnValueAsMap(final String text) throws AdapterException {
        final NameValueParser nvp = new NameValueParser(new NVPProperty(this.prop.getMap()));
        if (text.charAt(0) == this.prop.quotecharacter && text.charAt(text.length() - 1) == this.prop.quotecharacter) {
            final String columnValue = text.substring(1, text.length() - 1);
            return nvp.convertToMap(columnValue);
        }
        return nvp.convertToMap(text);
    }
    
    @Override
    public List<String> applyRegexOnColumnValue(final String columnValue, final String regex) {
        return null;
    }
    
    @Override
    public void close() throws AdapterException {
        super.close();
        this.rowData = null;
    }
}
