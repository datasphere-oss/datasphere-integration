package com.datasphere.utils.writers.common;

import org.apache.log4j.*;
import java.util.*;

public class ParserUtility
{
    private static Logger logger;
    
    public static Map<String, String> get1PartTables(final String tableString) {
        final Map<String, String> tablePairMap = new HashMap<String, String>();
        int colmap = 0;
        if (!tableString.trim().isEmpty()) {
            final String[] split;
            final String[] tableMaps = split = tableString.split(";");
            for (final String tableMap : split) {
                final String tableMapCopy = new String(tableMap).toUpperCase();
                final String[] tableMapSplits = tableMap.split(",");
                if (tableMapCopy.contains("COLUMNMAP(")) {
                    colmap = 1;
                }
                else if (tableMapSplits.length != 2) {
                    throw new IllegalArgumentException("this format is not correct " + tableMap + ", please use the format src1,target1;src2,target2;...");
                }
                final String srcTable = tableMapSplits[0];
                String targetTableFamilyMap = null;
                if (colmap == 0) {
                    targetTableFamilyMap = tableMapSplits[1];
                }
                else {
                    final String[] targetTable = tableMapSplits[1].split("\\s+");
                    targetTableFamilyMap = targetTable[0];
                    colmap = 0;
                }
                tablePairMap.put(srcTable.trim(), targetTableFamilyMap.trim());
            }
            return tablePairMap;
        }
        throw new IllegalArgumentException("Please specify Tables info");
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
    
    public static Map<String, String> getNameValuePairs(final String text) {
        final Map<String, String> result = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        final String[] split2;
        final String[] textSplits = split2 = text.split(";");
        for (final String split : split2) {
            final String[] supersplit = split.split("->");
            if (supersplit.length != 2) {
                throw new IllegalArgumentException("The value should be in format key1->val1;key2->val2,val3; etc");
            }
            final String key = supersplit[0];
            final String value = supersplit[1];
            result.put(key, value);
        }
        return result;
    }
    
    static {
        ParserUtility.logger = Logger.getLogger((Class)ParserUtility.class);
    }
}
