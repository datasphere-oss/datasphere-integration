package com.datasphere.utility;

import java.util.*;

public class CDCTablesProperty extends HashMap<String, Map<ParserUtility.ColumnGroupTypes, List<String>>>
{
    private static final long serialVersionUID = 1L;
    
    public List<String> getListOfColumns(final String tableName, final ParserUtility.ColumnGroupTypes groupType) {
        if (this.containsKey(tableName) && this.get(tableName).containsKey(groupType)) {
            return this.get(tableName).get(groupType);
        }
        return new ArrayList<String>();
    }
    
    void setListOfColumns(final String tableName, final ParserUtility.ColumnGroupTypes groupType, final List<String> columns) {
        if (this.containsKey(tableName)) {
            this.get(tableName).put(groupType, columns);
        }
        else {
            final Map<ParserUtility.ColumnGroupTypes, List<String>> value = new HashMap<ParserUtility.ColumnGroupTypes, List<String>>();
            value.put(groupType, columns);
            this.put(tableName, value);
        }
    }
    
    public Map<String, List<String>> getTableMap(final ParserUtility.ColumnGroupTypes type) {
        final Map<String, List<String>> result = new HashMap<String, List<String>>();
        for (final Map.Entry<String, Map<ParserUtility.ColumnGroupTypes, List<String>>> entry : this.entrySet()) {
            if (entry.getValue().containsKey(type)) {
                result.put(entry.getKey(), entry.getValue().get(type));
            }
        }
        return result;
    }
    
    public Map<String, Map<ParserUtility.ColumnGroupTypes, List<String>>> getTableProperties() {
        return this;
    }
}
