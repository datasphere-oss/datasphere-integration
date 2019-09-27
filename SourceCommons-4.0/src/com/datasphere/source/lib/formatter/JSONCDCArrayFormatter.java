package com.datasphere.source.lib.formatter;

import java.lang.reflect.*;
import java.util.*;

import com.datasphere.runtime.*;
import com.datasphere.proc.events.*;
import com.datasphere.common.constants.*;
import org.apache.commons.lang3.*;
import org.codehaus.jettison.json.*;

public class JSONCDCArrayFormatter extends CDCArrayFormatter
{
    private String jsonMemberDelimiter;
    
    public JSONCDCArrayFormatter(final Field field, final Map<String, Object> properties) {
        super(field);
        this.jsonMemberDelimiter = "\n";
        final String jsonMemberDelimiter = (String)properties.get("jsonMemberDelimiter");
        if (jsonMemberDelimiter != null) {
            this.jsonMemberDelimiter = jsonMemberDelimiter;
        }
    }
    
    @Override
    public String formatCDCArray(final HDEvent waEvent, final Object[] dataOrBeforeArray, final Field[] fields) {
        boolean addComma = false;
        final int lastIndex = dataOrBeforeArray.length - 1;
        if (lastIndex == -1) {
            return "{}";
        }
        final StringBuilder b = new StringBuilder();
        b.append("{" + this.jsonMemberDelimiter);
        for (int i = 0; i <= lastIndex; ++i) {
            final boolean isPresent = BuiltInFunc.IS_PRESENT(waEvent, dataOrBeforeArray, i);
            if (fields != null) {
                if (isPresent) {
                    if (addComma) {
                        b.append("," + this.jsonMemberDelimiter);
                    }
                    final String operationName = (String)BuiltInFunc.META(waEvent, Constant.OPERATION_TYPE);
                    String columnName;
                    if (!operationName.isEmpty() && operationName.equalsIgnoreCase(Constant.DDL_OPERATION)) {
                        columnName = "DDLCommand";
                    }
                    else {
                        columnName = fields[i].getName();
                    }
                    this.appendValue(columnName, dataOrBeforeArray[i], b, ClassUtils.isPrimitiveOrWrapper((Class)fields[i].getType()));
                    if (!addComma) {
                        addComma = true;
                    }
                }
            }
            else {
                if (addComma) {
                    b.append("," + this.jsonMemberDelimiter);
                }
                this.appendValue(i, dataOrBeforeArray[i], b, false);
                if (!addComma) {
                    addComma = true;
                }
            }
            if (i == lastIndex) {
                break;
            }
        }
        return b.append(this.jsonMemberDelimiter + "}").toString();
    }
    
    private void appendValue(final Object memberName, final Object memberValue, final StringBuilder b, final boolean isPrimitive) {
        b.append("\"" + memberName + "\":");
        Object value = memberValue;
        if (value == null) {
            value = "null";
        }
        else if (!isPrimitive) {
            value = JSONObject.quote(value.toString());
        }
        b.append(value);
    }
}
