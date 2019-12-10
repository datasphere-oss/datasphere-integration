package com.datasphere.source.lib.formatter;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.datasphere.runtime.BuiltInFunc;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.source.lib.formatter.dsv.util.DSVFormatterUtil;

public class DSVCDCArrayFormatter extends CDCArrayFormatter
{
    private DSVFormatterUtil dsvFormatterUtil;
    private String columnDelimiter;
    private String nullValue;
    
    public DSVCDCArrayFormatter(final Field field, final Map<String, Object> properties) {
        super(field);
        this.dsvFormatterUtil = new DSVFormatterUtil(properties);
        this.columnDelimiter = this.dsvFormatterUtil.getColumnDelimiter();
        this.nullValue = this.dsvFormatterUtil.getNullValue();
    }
    
    @Override
    public String formatCDCArray(final HDEvent hdEvent, final Object[] dataOrBeforeArray, final Field[] fields) {
        final int lastIndex = dataOrBeforeArray.length - 1;
        if (lastIndex == -1) {
            return "";
        }
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i <= lastIndex; ++i) {
            if (fields != null) {
                final boolean isPresent = BuiltInFunc.IS_PRESENT(hdEvent, dataOrBeforeArray, i);
                if (isPresent) {
                    this.appendValue(dataOrBeforeArray[i], b);
                }
                else {
                    b.append("");
                }
            }
            else {
                this.appendValue(dataOrBeforeArray[i], b);
            }
            if (i == lastIndex) {
                break;
            }
            b.append(this.columnDelimiter);
        }
        return b.toString();
    }
    
    private void appendValue(Object value, final StringBuilder b) {
        if (value == null) {
            value = this.nullValue;
        }
        if (value instanceof byte[]) {
            value = Base64.encodeBase64String((byte[])value);
        }
        b.append(this.dsvFormatterUtil.quoteValue(value.toString()));
    }
}
