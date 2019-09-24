package com.datasphere.runtime.utils;

import org.apache.log4j.*;
import java.util.*;

public class StringUtils
{
    private static Logger logger;
    
    public static String join(final List<?> list, final String separator) {
        if (list.size() == 1) {
            return list.get(0).toString();
        }
        final StringBuilder sb = new StringBuilder();
        String sep = "";
        for (final Object o : list) {
            sb.append(sep).append(o);
            sep = separator;
        }
        return sb.toString();
    }
    
    public static String join(final List<?> list) {
        return join(list, ",");
    }
    
    public static <T> String join(final T[] list) {
        return join(list, ",");
    }
    
    public static String join(final Object[] list, final String separator) {
        final StringBuilder sb = new StringBuilder();
        String sep = "";
        for (final Object o : list) {
            sb.append(sep).append(o);
            sep = separator;
        }
        return sb.toString();
    }
    
    public static Map<String, Object> getKeyValuePairs(final String inputStr, String itemSplitter, String keyValSplitter) {
        final Map<String, Object> results = MapFactory.makeCaseInsensitiveMap();
        if (inputStr == null || inputStr.isEmpty()) {
            return results;
        }
        if (itemSplitter == null) {
            itemSplitter = " ";
        }
        final String[] items = inputStr.split(itemSplitter);
        if (keyValSplitter == null) {
            keyValSplitter = "=";
        }
        for (final String item : items) {
            final String[] parts = item.split(keyValSplitter);
            if (parts != null && parts.length == 2 && parts[0] != null && parts[1] != null) {
                results.put(parts[0].trim(), parts[1].trim());
            }
        }
        return results;
    }
    
    public static int getInt(final Map<String, Object> map, final String key) throws ClassCastException, NumberFormatException, NullPointerException {
        int c = 0;
        final Object val = map.get(key);
        if (val instanceof Number) {
            c = ((Number)val).intValue();
        }
        else if (val instanceof String) {
            c = Integer.parseInt((String)val);
        }
        return c;
    }
    
    public static List<String> getEnumValuesAsList(final String enumName) throws Exception {
        final List<String> values = new ArrayList<String>();
        if (enumName == null || enumName.isEmpty()) {
            return values;
        }
        try {
            final Class enumClass = Class.forName(enumName);
            final Object[] enumValsArray = enumClass.getEnumConstants();
            if (enumValsArray != null && enumValsArray.length > 0) {
                for (final Object enumVal : enumValsArray) {
                    values.add(((Enum)enumVal).name());
                }
            }
        }
        catch (Exception ex) {
            StringUtils.logger.error((Object)("error while getting enum value for : " + enumName), (Throwable)ex);
            throw new Exception("error while getting enum value for : " + enumName, ex);
        }
        return values;
    }
    
    public static String getListValuesAsString(final List<String> list, String delimiter) {
        if (list == null) {
            return null;
        }
        if (list.isEmpty()) {
            return "";
        }
        if (delimiter == null) {
            delimiter = "";
        }
        final StringBuffer sbuffer = new StringBuffer();
        for (final String str : list) {
            sbuffer.append(str).append(delimiter);
        }
        String str2 = sbuffer.toString();
        str2 = (String)str2.subSequence(0, str2.length() - delimiter.length());
        return str2;
    }
    
    public static byte[] hexToRaw(final String hexString) {
        byte[] buffer = null;
        final char[] hexData = hexString.toCharArray();
        buffer = new byte[hexData.length / 2];
        int byteIdx = 0;
        byte byteVal = 0;
        int intValue = 0;
        for (int itr = 0; itr < hexData.length; ++itr) {
            switch (hexData[itr]) {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9': {
                    intValue = hexData[itr] - '0';
                    break;
                }
                case 'a':
                case 'b':
                case 'c':
                case 'd':
                case 'e':
                case 'f': {
                    intValue = hexData[itr] - 'a' + '\n';
                    break;
                }
                case 'A':
                case 'B':
                case 'C':
                case 'D':
                case 'E':
                case 'F': {
                    intValue = hexData[itr] - 'A' + '\n';
                    break;
                }
                default: {
                    intValue = hexData[itr];
                    break;
                }
            }
            if (itr % 2 != 0) {
                byteVal <<= 4;
                byteVal |= (byte)(intValue & 0xF);
                buffer[byteIdx++] = byteVal;
            }
            else {
                byteVal = 0;
                byteVal |= (byte)(intValue & 0xF);
            }
        }
        return buffer;
    }
    
    static {
        StringUtils.logger = Logger.getLogger((Class)StringUtils.class);
    }
}
