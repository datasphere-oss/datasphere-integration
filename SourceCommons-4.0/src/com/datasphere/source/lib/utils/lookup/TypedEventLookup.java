package com.datasphere.source.lib.utils.lookup;

import java.lang.reflect.*;
import com.datasphere.event.*;
import java.util.*;

public class TypedEventLookup implements EventLookup
{
    private Map<String, Field> tokenNameFieldMap;
    
    public TypedEventLookup(final Map<String, Field> tokenNameFieldMap) {
        this.tokenNameFieldMap = null;
        this.tokenNameFieldMap = tokenNameFieldMap;
    }
    
    @Override
    public List<Object> get(final Event event) throws Exception {
        final List<Object> dataList = new ArrayList<Object>();
        for (final Map.Entry<String, Field> entry : this.tokenNameFieldMap.entrySet()) {
            dataList.add(entry.getValue().get(event));
        }
        return dataList;
    }
}
