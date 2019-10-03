package com.datasphere.proc.Table;

import java.util.*;
import com.datasphere.uuid.UUID;
import com.datasphere.Tables.*;
import com.datasphere.common.exc.*;

public class SourceTableMap
{
    Map<String, Table> tableMap;
    
    public SourceTableMap() {
        this.tableMap = new HashMap<String, Table>();
    }
    
    public void addTable(final Table sourceTable) {
        this.tableMap.put(sourceTable.getName(), sourceTable);
    }
    
    public Table getTableMeta(final UUID sourceUUID, final String alias) throws AdapterException {
        Table srcTbl = this.tableMap.get(sourceUUID.toString());
        if (srcTbl == null) {
            srcTbl = (Table)new UUIDTable(sourceUUID, alias);
            this.tableMap.put(sourceUUID.toString(), srcTbl);
            this.tableMap.put(alias, srcTbl);
        }
        return srcTbl;
    }
    
    public Table getTableMeta(final String tableName) {
        return this.tableMap.get(tableName);
    }
}

