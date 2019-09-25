package com.datasphere.Table;

import java.util.HashMap;
import java.util.Map;

import com.datasphere.uuid.UUID;

public class SourceTableMap
{
    public static Map<String, SourceTableMap> sourceTableMap;
    Map<String, Table> tableMap;
    
    public SourceTableMap() {
        this.tableMap = new HashMap<String, Table>();
    }
    
    public void addTable(final Table sourceTable) {
        this.tableMap.put(sourceTable.getName(), sourceTable);
    }
    
    public Table getTableMeta(final UUID sourceUUID, final String alias) {
        Table srcTbl = this.tableMap.get(sourceUUID.toString());
        if (srcTbl == null) {
            srcTbl = new UUIDTable(sourceUUID, alias);
            this.tableMap.put(sourceUUID.toString(), srcTbl);
            this.tableMap.put(alias, srcTbl);
        }
        return srcTbl;
    }
    
    public Table getTableMeta(final String tableName) {
        return this.tableMap.get(tableName);
    }
    
    static {
        SourceTableMap.sourceTableMap = new HashMap<String, SourceTableMap>();
    }
}
