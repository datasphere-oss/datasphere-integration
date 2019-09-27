package com.datasphere.TypeHandlers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.datasphere.uuid.UUID;
import com.datasphere.Exceptions.UnsupportedMappingException;
import com.datasphere.Tables.Table;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.source.lib.meta.DatabaseColumn;

public class TableToTypeHandlerMap
{
    private static final String SEPARATOR = "-";
    private static Logger logger;
    Map<String, TypeHandler[]> tableColumnMapping;
    Map<String, ArrayList<String>> targetTableRefMap;
    TargetTypeHandler handler;
    
    public TableToTypeHandlerMap(final String targetType) {
        this.handler = TargetTypeHandlerFactory.getTargetTypeHandler(targetType);
        this.tableColumnMapping = new TreeMap<String, TypeHandler[]>(String.CASE_INSENSITIVE_ORDER);
        this.targetTableRefMap = new HashMap<String, ArrayList<String>>();
    }
    
    public TypeHandler[] getColumnMapping(final String referenceKey) {
        return this.tableColumnMapping.get(referenceKey);
    }
    
    public void initializeForTable(final String referenceKey, final Table targetTable, final String tableType) {
        if (this.tableColumnMapping.get(referenceKey) == null) {
            TableToTypeHandlerMap.logger.debug((Object)("Initialzing table structure for {" + referenceKey + "}"));
            final DatabaseColumn[] columnList = targetTable.getColumns();
            final TypeHandler[] columns = new TypeHandler[columnList.length];
            for (int itr = 0; itr < columns.length; ++itr) {
                final int sqlType = this.handler.getSqlType(columnList[itr].typeCode, columnList[itr]);
                columns[itr] = new TypeHandler(sqlType, tableType);
            }
            this.tableColumnMapping.put(referenceKey, columns);
            TableToTypeHandlerMap.logger.debug((Object)("Initialized table structure for {" + referenceKey + "}. No Of Columns {" + columns.length + "}"));
            ArrayList<String> referenceList = this.targetTableRefMap.get(targetTable.getFullyQualifiedName());
            if (referenceList == null) {
                referenceList = new ArrayList<String>();
            }
            referenceList.add(referenceKey);
            this.targetTableRefMap.put(targetTable.getFullyQualifiedName(), referenceList);
        }
        else {
            TableToTypeHandlerMap.logger.debug((Object)("Table is already initialized for Key {" + referenceKey + "}."));
        }
    }
    
    public void invalidateMappingFor(final String fullyQualifiedTarget) {
        final ArrayList<String> referenceKeys = this.targetTableRefMap.get(fullyQualifiedTarget);
        if (referenceKeys != null) {
            for (final String referenceKey : referenceKeys) {
                if (TableToTypeHandlerMap.logger.isDebugEnabled()) {
                    TableToTypeHandlerMap.logger.debug((Object)("Removing table map entry for {" + referenceKey + "}"));
                }
                this.tableColumnMapping.remove(referenceKey);
            }
            this.targetTableRefMap.remove(fullyQualifiedTarget);
        }
    }
    
    public TypeHandler[] updateColumnMapping(final String key, final int columnIdx, final Class<?> classType) throws UnsupportedMappingException {
        final TypeHandler[] columns = this.getColumnMapping(key);
        if (TableToTypeHandlerMap.logger.isDebugEnabled()) {
            TableToTypeHandlerMap.logger.debug((Object)("Updating column mappig for SQLType {" + columns[columnIdx].sqlType + "} with {" + classType.getName() + "}"));
        }
        if (columnIdx > columns.length) {
            TableToTypeHandlerMap.logger.error((Object)("Column index out of rage for key {" + key + "} Coloumn Count {" + columns.length + "} Received Index {" + columnIdx + "}"));
            return columns;
        }
        columns[columnIdx] = this.handler.getHandler(columns[columnIdx].sqlType, classType);
        this.tableColumnMapping.put(key, columns);
        return columns;
    }
    
    public static String formKey(final String tableName, final HDEvent event) {
        final String key = event.sourceUUID + "-" + event.typeUUID + "-" + tableName;
        return key;
    }
    
    public static String formKey(final String tableName, final UUID sourceUUID, final UUID typeUUID) {
        final String key = sourceUUID + "-" + typeUUID + "-" + tableName;
        return key;
    }
    
    static {
        TableToTypeHandlerMap.logger = Logger.getLogger((Class)TableToTypeHandlerMap.class);
    }
}
