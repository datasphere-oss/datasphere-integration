package com.datasphere.proc.Table;

import com.datasphere.source.cdc.common.*;
import com.datasphere.proc.Connection.*;
import com.datasphere.proc.exception.*;

import org.apache.log4j.*;
import java.util.*;
import com.datasphere.Tables.*;

import org.json.*;

public class TargetTableMap
{
    private Map<String, DatabaseTable> tableMetaMap;
    private Map<String, String> excludeList;
    private String targetId;
    private static Logger logger;
    
    public TargetTableMap() {
        this.tableMetaMap = new HashMap<String, DatabaseTable>();
        this.excludeList = new HashMap<String, String>();
    }
    
    public synchronized void updateTableRef(final Table updatedTableMeta) {
        final String name = updatedTableMeta.getName();
        final String fullyQualifiedName = updatedTableMeta.getFullyQualifiedName();
        this.tableMetaMap.put(name, (DatabaseTable)updatedTableMeta);
        this.tableMetaMap.put(fullyQualifiedName, (DatabaseTable)updatedTableMeta);
    }
    
    public synchronized DatabaseTable getTableMeta(final String tableName, final KuduWriterConnection connection) throws KuduWriterException {
        DatabaseTable tableDef = null;
        tableDef = this.tableMetaMap.get(tableName);
        if (tableDef == null && !this.excludeList.containsKey(tableName)) {
            try {
                tableDef = connection.getTableMeta(tableName);
                this.tableMetaMap.put(tableName, tableDef);
                final String fullyQualifiedName = tableDef.getFullyQualifiedName();
                this.tableMetaMap.put(fullyQualifiedName, tableDef);
            }
            catch (KuduWriterException exp) {
                if (exp.getErrorCode() == Error.TARGET_TABLE_DOESNOT_EXISTS.getType()) {
                    TargetTableMap.logger.warn((Object)("Table {" + tableName + "} does not exist, adding into exclusion list. Will not attempt to reload it"));
                    this.excludeList.put(tableName, tableName);
                }
                throw exp;
            }
        }
        return tableDef;
    }
    
    public synchronized void addNewTable(final String targetTbl, final JSONObject tableData) throws JSONException {
        final Table newTable = new Table(targetTbl, tableData);
        this.tableMetaMap.put(targetTbl, (DatabaseTable)newTable);
        if (this.excludeList.containsKey(targetTbl)) {
            this.excludeList.remove(targetTbl);
            if (TargetTableMap.logger.isInfoEnabled()) {
                TargetTableMap.logger.info((Object)("Removing {" + targetTbl + "} from exclusion list"));
            }
        }
    }
    
    public synchronized void addNewTable(final Table table) {
        this.tableMetaMap.put(table.getName(), (DatabaseTable)table);
    }
    
    public synchronized void updateTableDetails(final String targetTbl, final JSONObject tableData) throws JSONException {
        this.tableMetaMap.remove(targetTbl);
        this.addNewTable(targetTbl, tableData);
    }
    
    public synchronized void removeTableDetails(final String targetTbl) throws JSONException {
        this.tableMetaMap.remove(targetTbl);
    }
    
    static {
        TargetTableMap.logger = Logger.getLogger((Class)TargetTableMap.class);
    }
}
