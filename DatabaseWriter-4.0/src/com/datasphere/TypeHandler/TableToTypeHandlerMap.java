package com.datasphere.TypeHandler;

import org.slf4j.*;

import com.datasphere.source.lib.meta.*;
import com.datasphere.Table.*;
import com.datasphere.exception.*;
import com.datasphere.proc.events.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class TableToTypeHandlerMap
{
    private static final String SEPARATOR = "-";
    private static Logger logger;
    Map<String, TypeHandler[]> tableColumnMapping;
    Map<String, ArrayList<String>> targetTableRefMap;
    TargetTypeHandler handler;
    
    static {
        TableToTypeHandlerMap.logger = LoggerFactory.getLogger((Class)TableToTypeHandlerMap.class);
    }
    
    public TableToTypeHandlerMap(final String targetType) {
        this.handler = TargetTypeHandlerFactory.getTargetTypeHandler(targetType);
        this.tableColumnMapping = new TreeMap<String, TypeHandler[]>(String.CASE_INSENSITIVE_ORDER);
        this.targetTableRefMap = new HashMap<String, ArrayList<String>>();
    }
    
    public TypeHandler[] getColumnMapping(final String referenceKey) {
        return this.tableColumnMapping.get(referenceKey);
    }
    
    public void initializeForTable(final String referenceKey, final Table targetTable) {
        if (this.tableColumnMapping.get(referenceKey) == null) {
            TableToTypeHandlerMap.logger.debug("Initialzing table structure for {" + referenceKey + "}");
            final DatabaseColumn[] columnList = targetTable.getColumns();
            final TypeHandler[] columns = new TypeHandler[columnList.length];
            for (int itr = 0; itr < columns.length; ++itr) {
                final int sqlType = this.handler.getSqlType(columnList[itr].typeCode, columnList[itr].getPrecision());
                columns[itr] = new TypeHandler(sqlType);
            }
            this.tableColumnMapping.put(referenceKey, columns);
            TableToTypeHandlerMap.logger.debug("Initialized table structure for {" + referenceKey + "}. No Of Columns {" + columns.length + "}");
            ArrayList<String> referenceList = this.targetTableRefMap.get(targetTable.getFullyQualifiedName());
            if (referenceList == null) {
                referenceList = new ArrayList<String>();
            }
            referenceList.add(referenceKey);
            this.targetTableRefMap.put(targetTable.getFullyQualifiedName(), referenceList);
        }
        else {
            TableToTypeHandlerMap.logger.debug("Table is already initialized for Key {" + referenceKey + "}.");
        }
    }
    
    public void invalidateMappingFor(final String fullyQualifiedTarget) {
        final ArrayList<String> referenceKeys = this.targetTableRefMap.get(fullyQualifiedTarget);
        if (referenceKeys != null) {
            for (final String referenceKey : referenceKeys) {
                if (TableToTypeHandlerMap.logger.isDebugEnabled()) {
                    TableToTypeHandlerMap.logger.debug("Removing table map entry for {" + referenceKey + "}");
                }
                this.tableColumnMapping.remove(referenceKey);
            }
            this.targetTableRefMap.remove(fullyQualifiedTarget);
        }
    }
    
    public TypeHandler[] updateColumnMapping(final String key, final int columnIdx, final Class<?> classType) throws UnsupportedMapping {
        final TypeHandler[] columns = this.getColumnMapping(key);
        if (TableToTypeHandlerMap.logger.isDebugEnabled()) {
            TableToTypeHandlerMap.logger.debug("Updating column mappig for SQLType {" + columns[columnIdx].sqlType + "} with {" + classType.getName() + "}");
        }
        if (columnIdx > columns.length) {
            TableToTypeHandlerMap.logger.error("Column index out of rage for key {" + key + "} Coloumn Count {" + columns.length + "} Received Index {" + columnIdx + "}");
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
    
    public static void main(final String[] args) throws ClassNotFoundException, IOException {
        final Class[] classes3;
        final Class[] classes2 = classes3 = getClasses("com.datasphere.ObjectMapper");
        Class[] array;
        for (int length = (array = classes2).length, i = 0; i < length; ++i) {
            final Class<?> classObj = (Class<?>)array[i];
            System.out.println("Class {" + classObj.getName() + "}");
        }
    }
    
    private static Class<?>[] getClasses(final String packageName) throws ClassNotFoundException, IOException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        assert classLoader != null;
        final String path = packageName.replace('.', '/');
        final Enumeration resources = classLoader.getResources(path);
        final List<File> dirs = new ArrayList<File>();
        while (resources.hasMoreElements()) {
            final URL resource = (URL)resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }
        final ArrayList<Class<?>> classes = new ArrayList<Class<?>>();
        for (final File directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }
        return classes.toArray(new Class[classes.size()]);
    }
    
    private static List findClasses(final File directory, final String packageName) throws ClassNotFoundException {
        final List classes = new ArrayList();
        if (!directory.exists()) {
            return classes;
        }
        final File[] files;
        final File[] listFiles = files = directory.listFiles();
        File[] array;
        for (int length = (array = listFiles).length, i = 0; i < length; ++i) {
            final File file = array[i];
            if (file.isDirectory()) {
                assert !file.getName().contains(".");
                classes.addAll(findClasses(file, String.valueOf(packageName) + "." + file.getName()));
            }
            else if (file.getName().endsWith(".class")) {
                classes.add(Class.forName(String.valueOf(packageName) + '.' + file.getName().substring(0, file.getName().length() - 6)));
            }
        }
        return classes;
    }
}
