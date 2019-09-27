package com.datasphere.DBCommons;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.datasphere.proc.events.HDEvent;

public class ColumnExcluder
{
    private HashMap<String, ArrayList<Integer>> tableMap;
    private TableMetaProvider consumer;
    DatabaseMetaData metaData;
    
    public ColumnExcluder(final TableMetaProvider consumer, final Map<String, Object> properties) throws IllegalArgumentException, SQLException {
        this.tableMap = new HashMap<String, ArrayList<Integer>>();
        this.consumer = consumer;
        this.metaData = this.consumer.getMetaData();
        String tableName = null;
        ResultSet result = null;
        final String excludedColumns = properties.get("ExcludedColumn") == null ? null : properties.get("ExcludedColumn").toString();
        excludedColumns.replaceAll("\\s+", "");
        final String[] excludedColsForTables = excludedColumns.split(";");
        for (int i = 0; i < excludedColsForTables.length; ++i) {
            try {
                tableName = excludedColsForTables[i].substring(0, excludedColsForTables[i].indexOf("="));
                final ResultSet tableResultSet = this.metaData.getTables(null, null, tableName, new String[] { "TABLE", "VIEW", "ALIAS", "SYNONYM" });
                final String[] excludedCols = excludedColsForTables[i].substring(excludedColsForTables[i].indexOf("=") + 1).split(",");
                while (tableResultSet.next()) {
                    final String c = tableResultSet.getString(1);
                    final String s = tableResultSet.getString(2);
                    final String t = tableResultSet.getString(3);
                    result = this.metaData.getColumns(c, s, t, null);
                    final ArrayList<Integer> columns = new ArrayList<Integer>();
                    int indexCount = 0;
                    while (result.next()) {
                        if (this.search(excludedCols, result.getString(4))) {
                            columns.add(indexCount);
                        }
                        ++indexCount;
                    }
                    final StringBuilder tableFQN = new StringBuilder();
                    if (c != null && !c.trim().isEmpty()) {
                        tableFQN.append(c + ".");
                    }
                    if (s != null && !s.trim().isEmpty()) {
                        tableFQN.append(s + ".");
                    }
                    tableFQN.append(t);
                    this.tableMap.put(tableFQN.toString(), columns);
                }
            }
            catch (SQLException e) {
                throw e;
            }
        }
    }
    
    public HDEvent excludeColumns(final HDEvent waevent) {
        try {
            final String tableName = waevent.metadata.get("TableName") == null ? null : waevent.metadata.get("TableName").toString();
            for (int i = 0; i < this.tableMap.get(tableName).size(); ++i) {
                waevent.unsetData((int)this.tableMap.get(tableName).get(i));
            }
        }
        catch (Exception e) {
            throw e;
        }
        return waevent;
    }
    
    public boolean search(final String[] array, final String key) {
        for (int i = 0; i < array.length; ++i) {
            if (array[i].equals(key)) {
                return true;
            }
        }
        return false;
    }
}
