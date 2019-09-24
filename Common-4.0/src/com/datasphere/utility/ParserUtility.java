package com.datasphere.utility;

import org.apache.log4j.*;
import java.util.regex.*;
import java.util.*;

public class ParserUtility
{
    private static Logger logger;
    
    public static Map<String, String> get1PartTables(final String tableString, final boolean acceptOnlyKey) {
        final Map<String, String> tablePairMap = new HashMap<String, String>();
        if (!tableString.trim().isEmpty()) {
            final String[] split;
            final String[] tableMaps = split = tableString.split(";");
            for (final String tmpTableMap : split) {
                final String[] parts = tmpTableMap.split("((\\))[\\s]+(,))|((\\))(,))");
                if (acceptOnlyKey && parts.length == 1 && parts[0].indexOf(40) == -1) {
                    if (parts[0].indexOf(40) != -1 || parts[0].indexOf(41) != -1 || parts[0].indexOf(32) != -1) {
                        throw new IllegalArgumentException("Table property is not correctly formatted {" + tmpTableMap + "}");
                    }
                    tablePairMap.put(parts[0], "");
                }
                else {
                    String srcTable = "";
                    String lastProperty = "";
                    final Pattern tblPattern = Pattern.compile(".+?(?= )");
                    final Pattern propTypePattern = Pattern.compile("(?<= ).*(?=\\()");
                    final Pattern fieldPattern = Pattern.compile("(?<=\\().*(?=\\))");
                    final StringBuilder tableProperty = new StringBuilder();
                    for (String tableMap : parts) {
                        if (tableMap.indexOf(41) == -1) {
                            tableMap += ')';
                        }
                        if (srcTable.isEmpty()) {
                            final Matcher tblMatcher = tblPattern.matcher(tableMap);
                            if (!tblMatcher.find()) {
                                throw new IllegalArgumentException("Source table part seems to be missing {" + tmpTableMap + "}");
                            }
                            srcTable = tblMatcher.group();
                        }
                        final Matcher propMatcher = propTypePattern.matcher(tableMap);
                        if (!propMatcher.find()) {
                            throw new IllegalArgumentException("Expected table property but it seems to be missing {" + tmpTableMap + "}");
                        }
                        lastProperty = propMatcher.group();
                        tableProperty.append(lastProperty);
                        final Matcher fieldMatcher = fieldPattern.matcher(tableMap);
                        if (!fieldMatcher.find()) {
                            throw new IllegalArgumentException("Expected list of files for source table {" + srcTable + "} for property {" + lastProperty + "}  {" + tmpTableMap + "}");
                        }
                        tableProperty.append("(" + fieldMatcher.group() + ")");
                    }
                    tablePairMap.put(srcTable.trim(), tableProperty.toString());
                }
            }
        }
        return tablePairMap;
    }
    
    public static Map<String, String> get2PartTables(final String tableString) {
        final Map<String, String> tablePairMap = new HashMap<String, String>();
        if (!tableString.trim().isEmpty()) {
            final String[] split;
            final String[] tableMaps = split = tableString.split(";");
            for (final String tableMap : split) {
                final String[] tableMapSplits = tableMap.split(",");
                if (tableMapSplits.length > 2) {
                    throw new IllegalArgumentException("Too many tables, please use the format srcTable1,target1.cf1;srcTable2,target2.cf2;...");
                }
                if (tableMapSplits.length <= 1) {
                    throw new IllegalArgumentException("Please provide both source and target tables use the format format srcTable1,target1.cf1;srcTable2,target2.cf2;...");
                }
                final String srcTable = tableMapSplits[0];
                final String targetTableFamilyMap = tableMapSplits[1];
                tablePairMap.put(srcTable.trim(), targetTableFamilyMap.trim());
                final String[] hbasetbl = targetTableFamilyMap.split("\\.");
                if (hbasetbl.length == 2) {
                    if (hbasetbl[1].trim().length() == 0) {
                        throw new IllegalArgumentException("cf cannot be empty, Specify cf for target table " + hbasetbl[0].trim());
                    }
                }
                else {
                    if (hbasetbl.length == 0) {
                        throw new IllegalArgumentException("source table have empty map for target table, Must have table name and cf in format targettable.cf;");
                    }
                    if (hbasetbl.length == 1) {
                        throw new IllegalArgumentException(hbasetbl[0].trim() + " table  have empty map for cf, Must have table name with cf in format targettable.cf");
                    }
                    if (hbasetbl.length > 2) {
                        throw new IllegalArgumentException("Too many information, Follow the format use one target table and one columnfamily table " + hbasetbl[0].trim());
                    }
                }
            }
            return tablePairMap;
        }
        throw new IllegalArgumentException("Please specify Tables info");
    }
    
    public static Long[] getBatchPolicyAndBatchInterval(final String batchpolicystring, final long defaultBatchCount, final long defaultBatchInterval) {
        final Long[] values = new Long[2];
        final String[] batchPolicies = batchpolicystring.split(",");
        if (batchpolicystring.length() == 0) {
            values[0] = new Long(defaultBatchCount);
            values[1] = new Long(defaultBatchInterval);
            return values;
        }
        try {
            boolean isEventCount = false;
            boolean isInterval = false;
            for (final String batch : batchPolicies) {
                final String[] kv = batch.split(":");
                if (kv.length != 2) {
                    throw new IllegalArgumentException("Error parsing value " + batch);
                }
                if (kv[0].trim().equalsIgnoreCase("eventCount")) {
                    values[0] = Long.parseLong(kv[1]);
                    isEventCount = true;
                }
                else {
                    if (!kv[0].trim().equalsIgnoreCase("Interval")) {
                        throw new IllegalArgumentException("Error parsing value " + kv[0] + " is unexpected, use proper format for eg eventCount:<batchCount>,Interval:<no in sec>");
                    }
                    values[1] = Long.parseLong(kv[1]) * 1000L;
                    isInterval = true;
                }
            }
            if (!isEventCount) {
                ParserUtility.logger.warn((Object)("Setting eventCount as default: " + defaultBatchCount));
                values[0] = new Long(defaultBatchCount);
            }
            if (!isInterval) {
                ParserUtility.logger.warn((Object)("Setting Interval as default: " + defaultBatchInterval));
                values[1] = new Long(defaultBatchInterval);
            }
            return values;
        }
        catch (Exception ex) {
            ParserUtility.logger.warn((Object)("Couldnot parse values properly because " + ex.getMessage() + ", setting to default values"));
            values[0] = new Long(defaultBatchCount);
            values[1] = new Long(defaultBatchInterval);
            return values;
        }
    }
    
    public static Map<String, List<String>> parseTablesWithKeyCols(final String tableString) {
        final Map<String, List<String>> result = new HashMap<String, List<String>>();
        final Map<String, String> tableKeycolMap = get1PartTables(tableString, true);
        for (final Map.Entry<String, String> tableKeyCol : tableKeycolMap.entrySet()) {
            if (tableKeyCol.getKey().contains("%") && !tableKeyCol.getValue().isEmpty()) {
                throw new IllegalArgumentException("tables with wildcard cannot provide keycols info, please check at " + tableKeyCol.getKey());
            }
            final ArrayList<String> listOfKeys = new ArrayList<String>();
            if (tableKeyCol.getValue().isEmpty()) {
                result.put(tableKeyCol.getKey(), null);
            }
            else {
                String value = tableKeyCol.getValue();
                value = value.replaceAll("(?i)keycolumns", "").trim();
                value = value.replaceAll("\\(", "").trim().replaceAll("\\)", "").trim();
                final String[] split;
                final String[] splits = split = value.split(",");
                for (final String val : split) {
                    listOfKeys.add(val);
                }
                result.put(tableKeyCol.getKey(), listOfKeys);
            }
        }
        return result;
    }
    
    public static CDCTablesProperty parseCDCTablesProperty(final String tablesString) {
        final CDCTablesProperty property = new CDCTablesProperty();
        final Map<String, String> tablesKV = get1PartTables(tablesString, true);
        final LinkedList<String> tableList = new LinkedList<String>();
        for (final Map.Entry<String, String> kvpair : tablesKV.entrySet()) {
            final Map<ColumnGroupTypes, List<String>> value = new HashMap<ColumnGroupTypes, List<String>>();
            if (kvpair.getKey().contains("%") && !kvpair.getValue().trim().isEmpty()) {
                throw new IllegalArgumentException("Table names with wildcards cannot provide properties");
            }
            if (!kvpair.getValue().isEmpty()) {
                final String[] split;
                final String[] propertyStrings = split = kvpair.getValue().split("\\)");
                for (final String propertyString : split) {
                    final int to = propertyString.indexOf("(");
                    String key = propertyString.substring(0, to);
                    final String val = propertyString.substring(to).trim().replaceAll("\\(", "").trim().replaceAll("\\)", "").trim();
                    if (val.trim().isEmpty()) {
                        throw new IllegalArgumentException("Empty property specified");
                    }
                    final String[] str = val.split(",");
                    for (int itr = 0; itr < str.length; ++itr) {
                        str[itr] = str[itr].trim();
                        if (str[itr].contains(" ")) {
                            throw new IllegalArgumentException("Columns suppose to be delimited by comma (,) not by white space {" + str[itr] + "}");
                        }
                        if (str[itr].isEmpty()) {
                            throw new IllegalArgumentException("Empty column name specified");
                        }
                    }
                    if (key.isEmpty()) {
                        key = ColumnGroupTypes.OTHERS.toString();
                    }
                    final ColumnGroupTypes propGroup = ColumnGroupTypes.valueOf(key.trim().toUpperCase());
                    if (value.containsKey(propGroup)) {
                        throw new IllegalArgumentException("Proerty {" + propGroup.toString() + "} is specified more than once");
                    }
                    value.put(ColumnGroupTypes.valueOf(key.trim().toUpperCase()), Arrays.asList(str));
                }
            }
            property.put(kvpair.getKey(), value);
            tableList.add(kvpair.getKey());
        }
        return property;
    }
    
    static {
        ParserUtility.logger = Logger.getLogger((Class)ParserUtility.class);
    }
    
    public enum ColumnGroupTypes
    {
        KEYCOLUMNS, 
        FETCHCOLS, 
        REPLACEBADCOLUMNS, 
        EXCLUDEDCOLUMNS, 
        INCLUDECOLUMNS, 
        OTHERS;
    }
}
