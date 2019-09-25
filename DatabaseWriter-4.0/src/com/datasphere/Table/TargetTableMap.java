package com.datasphere.Table;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.source.cdc.common.DatabaseTable;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.Error;
import com.datasphere.intf.DBInterface;

public class TargetTableMap
{
    private static Map<String, TargetTableMap> targetToTableMap;
    private Map<String, DatabaseTable> tableMetaMap;
    private Map<String, String> excludeList;
    private DBInterface intf;
    private String targetId;
    private static Logger logger;
    
    static {
        TargetTableMap.targetToTableMap = new HashMap<String, TargetTableMap>();
        TargetTableMap.logger = LoggerFactory.getLogger((Class)TargetTableMap.class);
    }
    
    public static synchronized TargetTableMap getInstance(final String targetId, final DBInterface intf) {
        TargetTableMap ref = TargetTableMap.targetToTableMap.get(targetId);
        if (ref == null) {
            ref = TargetTableMap.targetToTableMap.get(Integer.toString(intf.hashCode()));
            if (ref == null) {
                ref = new TargetTableMap(targetId, intf);
                TargetTableMap.targetToTableMap.put(targetId, ref);
                TargetTableMap.targetToTableMap.put(Integer.toString(intf.hashCode()), ref);
            }
        }
        return ref;
    }
    
    public static TargetTableMap getInstance(final DBInterface intf) {
        return TargetTableMap.targetToTableMap.get(Integer.toString(intf.hashCode()));
    }
    
    public static TargetTableMap getInstance(final String targetId) {
        return TargetTableMap.targetToTableMap.get(targetId);
    }
    
    private TargetTableMap(final String target, final DBInterface dbInterface) {
        this.tableMetaMap = new HashMap<String, DatabaseTable>();
        this.excludeList = new HashMap<String, String>();
        this.intf = dbInterface;
        this.targetId = target;
    }
    
    public synchronized DatabaseTable getTableMeta(final String tableName) throws DatabaseWriterException {
        DatabaseTable tableDef = null;
        tableDef = this.tableMetaMap.get(tableName);
        if (tableDef == null && !this.excludeList.containsKey(tableName)) {
            try {
                tableDef = this.intf.getTableMeta(tableName);
                this.tableMetaMap.put(tableName, tableDef);
                final Table tbl = (Table)tableDef;
                final String fullyQualifiedName = tbl.getFullyQualifiedName();
                this.tableMetaMap.put(fullyQualifiedName, tableDef);
            }
            catch (DatabaseWriterException exp) {
                if (exp.getErrorCode() == Error.TARGET_TABLE_DOESNOT_EXISTS.getType()) {
                    TargetTableMap.logger.warn("Table {" + tableName + "} does not exist, adding into exclusion list. Will not attempt to reload it");
                    this.excludeList.put(tableName, tableName);
                }
                throw exp;
            }
        }
        return tableDef;
    }
    
    public synchronized void addNewTable(final String targetTbl, final JSONObject tableData) throws JSONException {
        final Table newTable = new Table(targetTbl, tableData);
        this.tableMetaMap.put(targetTbl, newTable);
        if (this.excludeList.containsKey(targetTbl)) {
            this.excludeList.remove(targetTbl);
            if (TargetTableMap.logger.isInfoEnabled()) {
                TargetTableMap.logger.info("Removing {" + targetTbl + "} from exclusion list");
            }
        }
    }
    
    public synchronized void updateTableDetails(final String targetTbl, final JSONObject tableData) throws JSONException {
        this.tableMetaMap.remove(targetTbl);
        this.addNewTable(targetTbl, tableData);
    }
    
    public synchronized void removeTableDetails(final String targetTbl) throws JSONException {
        this.tableMetaMap.remove(targetTbl);
    }
    
    public synchronized void clear() {
        this.tableMetaMap.clear();
        TargetTableMap.targetToTableMap.remove(this.targetId);
    }
    
    public void printMap() {
        for (final String key : this.tableMetaMap.keySet()) {
            System.out.println("Key: " + key + " Value: " + this.tableMetaMap.get(key));
        }
    }
}
