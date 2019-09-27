package com.datasphere.source.lib.formatter;

import org.codehaus.jettison.json.*;

import com.datasphere.source.lib.utils.*;

public class JSONStringValueFormatter extends FieldModifier
{
    @Override
    public String modifyFieldValue(final Object fieldValue, final Object event) throws Exception {
        String value = fieldValue.toString();
        value = JSONObject.quote(value);
        return value;
    }
}
