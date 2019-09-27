package com.datasphere.source.lib.formatter;

import java.lang.reflect.*;

import org.dom4j.tree.*;
import com.datasphere.runtime.*;
import com.datasphere.proc.events.*;

public class XMLCDCArrayFormatter extends CDCArrayFormatter
{
    public XMLCDCArrayFormatter(final Field field) {
        super(field);
    }
    
    @Override
    public String formatCDCArray(final HDEvent event, final Object[] dataOrBeforeArray, final Field[] fields) {
        final int lastIndex = dataOrBeforeArray.length - 1;
        final DefaultElement element = new DefaultElement("event-data");
        if (fields != null) {
            for (int i = 0; i <= lastIndex; ++i) {
                final boolean isPresent = BuiltInFunc.IS_PRESENT(event, dataOrBeforeArray, i);
                if (isPresent) {
                    if (dataOrBeforeArray[i] != null) {
                        element.addAttribute(fields[i].getName(), dataOrBeforeArray[i].toString());
                    }
                    else {
                        element.addAttribute(fields[i].getName(), "null");
                    }
                }
            }
        }
        return element.asXML();
    }
}
