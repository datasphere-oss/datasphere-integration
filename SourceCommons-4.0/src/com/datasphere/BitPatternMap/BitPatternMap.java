package com.datasphere.BitPatternMap;

import org.apache.log4j.*;

import com.datasphere.Tables.*;
import com.datasphere.source.lib.meta.*;
import com.datasphere.utils.writers.common.*;

import java.util.*;

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
    
    private static synchronized Integer[] initializePattern(final EventDataObject event, final byte[] bitMap) {
        Integer[] pattern = null;
        int count = 0;
        Object[] data;
        if (bitMap == event.beforePresenceBitMap) {
            data = event.before;
        }
        else {
            data = event.data;
        }
        final Integer[] tmpPattern = new Integer[data.length];
        int index = 0;
        for (int itr = 0; itr < data.length; ++itr) {
            if (IS_PRESENT(event, data, itr)) {
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
    
    public static synchronized Integer[] getPattern(final EventDataObject event, final byte[] bitMap) {
        final String bitMapKey = toString(bitMap);
        Integer[] pattern = BitPatternMap.patternMap.get(bitMapKey);
        if (pattern == null) {
            pattern = initializePattern(event, bitMap);
            BitPatternMap.patternMap.put(bitMapKey, pattern);
        }
        return pattern;
    }
    
    public static synchronized Integer[] getPattern(final EventDataObject event, final byte[] bitMap, final Table srcTbl, final Table tgtTbl, final boolean includeUnmangedColumns) {
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
    
    public static boolean IS_PRESENT(final EventDataObject e, final Object[] p_array, final int index) {
        final int pos = index / 7;
        final int offset = index % 7;
        int val = 0;
        final byte b = (byte)(1 << offset);
        if (p_array == e.data) {
            val = (e.dataPresenceBitMap[pos] & b) >> offset;
        }
        else if (p_array == e.before) {
            val = (e.beforePresenceBitMap[pos] & b) >> offset;
        }
        return val == 1;
    }
    
    static {
        BitPatternMap.patternMap = new HashMap<String, Integer[]>();
        BitPatternMap.logger = Logger.getLogger((Class)BitPatternMap.class);
        BitPatternMap.bitPtrnToIdxMap = new HashMap<String, Map<Integer, Boolean>>();
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
