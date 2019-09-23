package com.datasphere.runtime.converters;

import org.eclipse.persistence.mappings.converters.*;
import org.eclipse.persistence.sessions.*;
import java.util.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class StringSplitter implements Converter
{
    private static final long serialVersionUID = -3662523612151079504L;
    
    public Object convertDataValueToObjectValue(final Object dataval, final Session arg1) {
        if (dataval instanceof String) {
            final List<String> slist = new ArrayList<String>();
            final String[] sval = ((String)dataval).split(",");
            for (int i = 0; i < sval.length; ++i) {
                slist.add(sval[i]);
            }
            return slist;
        }
        return null;
    }
    
    public Object convertObjectValueToDataValue(final Object slist, final Session arg1) {
        if (slist instanceof List) {
            String sval = "";
            final List<String> strlist = (List<String>)slist;
            for (int i = 0; i < strlist.size(); ++i) {
                sval += strlist.get(i);
                if (i + 1 != strlist.size()) {
                    sval += ",";
                }
            }
            return sval;
        }
        return null;
    }
    
    public void initialize(final DatabaseMapping arg0, final Session arg1) {
    }
    
    public boolean isMutable() {
        return false;
    }
}
