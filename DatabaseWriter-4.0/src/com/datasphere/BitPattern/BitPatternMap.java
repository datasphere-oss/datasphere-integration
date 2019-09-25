package com.datasphere.BitPattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.runtime.BuiltInFunc;
import com.datasphere.source.lib.meta.DatabaseColumn;
import com.datasphere.Table.Table;
import com.datasphere.proc.events.HDEvent;

public class BitPatternMap
{
    private static Map<String, Integer[]> patternMap;
    private static Logger logger;
    private static Map<String, Map<Integer, Boolean>> bitPtrnToIdxMap;
    
    public static String toString(final byte[] data) {
        final StringBuilder builder = new StringBuilder();
        for (int itr = 0; itr < data.length; ++itr) {
            if (data[itr] != 0) {
                builder.append((char)data[itr]);
            }
            else {
                builder.append('\u0080');
            }
        }
        return builder.toString();
    }
    
    private static synchronized Integer[] initializePattern(final HDEvent event, final byte[] bitMap) {
        Integer[] pattern = null;
        int count = 0;
        Object[] data;
        if (bitMap == event.beforePresenceBitMap && event.before != null) {
            data = event.before;
        }
        else {
            data = event.data;
        }
        final Integer[] tmpPattern = new Integer[data.length];
        int index = 0;
        for (int itr = 0; itr < data.length; ++itr) {
            if (BuiltInFunc.IS_PRESENT(event, data, itr)) {
                tmpPattern[index++] = itr;
                ++count;
            }
        }
        pattern = new Integer[count];
        for (int itr = 0; itr < count; ++itr) {
            pattern[itr] = tmpPattern[itr];
        }
        return pattern;
    }
    
    public static synchronized Integer[] getPattern(final HDEvent event, final byte[] bitMap) {
        final String bitMapKey = new String(bitMap);
        Integer[] pattern = BitPatternMap.patternMap.get(bitMapKey);
        if (pattern == null) {
            pattern = initializePattern(event, bitMap);
            BitPatternMap.patternMap.put(bitMapKey, pattern);
        }
        return pattern;
    }
    
    public static synchronized Integer[] getPattern(final HDEvent event, final byte[] bitMap, final Table srcTbl, final Table tgtTbl, final boolean includeUnmangedColumns) {
        final String bitMapKey = toString(bitMap);
        final Integer[] pattern = getPattern(event, bitMap);
        final DatabaseColumn[] targetCols = tgtTbl.getColumns();
        final ArrayList<Integer> pattenLst = new ArrayList<Integer>();
        Map<Integer, Boolean> bitPatternAsMap = BitPatternMap.bitPtrnToIdxMap.get(bitMapKey);
        if (bitPatternAsMap == null) {
            bitPatternAsMap = new TreeMap<Integer, Boolean>();
            for (int itr = 0; itr < pattern.length; ++itr) {
                bitPatternAsMap.put(pattern[itr], true);
            }
            BitPatternMap.bitPtrnToIdxMap.put(bitMapKey, bitPatternAsMap);
        }
        for (int itr = 0; itr < targetCols.length; ++itr) {
            final int idx = targetCols[itr].getIndex();
            if (idx < 0) {
                if (includeUnmangedColumns) {
                    pattenLst.add(itr);
                }
            }
            else {
                final int srcColIdx = targetCols[itr].getIndex();
                if (bitPatternAsMap.containsKey(srcColIdx)) {
                    pattenLst.add(itr);
                }
            }
        }
        final Integer[] newPatten = new Integer[pattenLst.size()];
        for (int itr2 = 0; itr2 < pattenLst.size(); ++itr2) {
            newPatten[itr2] = pattenLst.get(itr2);
        }
        return newPatten;
    }
    
    static {
        BitPatternMap.patternMap = new HashMap<String, Integer[]>();
        BitPatternMap.logger = LoggerFactory.getLogger((Class)BitPatternMap.class);
        BitPatternMap.bitPtrnToIdxMap = new HashMap<String, Map<Integer, Boolean>>();
    }
}
