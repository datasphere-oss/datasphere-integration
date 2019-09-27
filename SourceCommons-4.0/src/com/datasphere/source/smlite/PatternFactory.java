package com.datasphere.source.smlite;

import java.util.*;

public class PatternFactory
{
    public static final String DEFAULT_PATTERN = "Pattern";
    public static final String DATE_PATTERN = "DatePattern";
    public static final String IP_PATTERN = "IPPattern";
    static Map<String, String> typeMap;
    static Map<String, Pattern> patternMap;
    
    static synchronized Pattern createPattern(final String patternString) {
        final String type = getPatternType(patternString);
        Pattern pattern = PatternFactory.patternMap.get(type + patternString);
        if (pattern == null) {
            pattern = PatternFactory.patternMap.get(type);
            pattern = (Pattern)pattern.clone();
            if (pattern == null) {
                return pattern;
            }
            pattern.init(patternString);
            PatternFactory.patternMap.put(type + patternString, pattern);
        }
        return pattern;
    }
    
    static String getPatternType(final String patternString) {
        String pattern = "Pattern";
        if (patternString.length() > 1) {
            final int startIdx = patternString.indexOf(37);
            if (startIdx != -1) {
                final int endIndex = patternString.indexOf(37, startIdx + 1);
                if (endIndex != -1) {
                    if (endIndex - startIdx > 2 && patternString.indexOf(37, endIndex + 1) == -1) {
                        String tmpType = patternString.substring(startIdx + 1, endIndex);
                        tmpType = PatternFactory.typeMap.get(tmpType);
                        if (tmpType != null) {
                            pattern = tmpType;
                        }
                    }
                    else {
                        pattern = "DatePattern";
                    }
                }
            }
        }
        return pattern;
    }
    
    static {
        PatternFactory.typeMap = new HashMap<String, String>() {
            {
                this.put("IP", "IPPattern");
            }
        };
        PatternFactory.patternMap = new HashMap<String, Pattern>() {
            {
                this.put("Pattern", new Pattern());
                this.put("IPPattern", new IPPattern());
                this.put("DatePattern", new DatePattern());
            }
        };
    }
}
