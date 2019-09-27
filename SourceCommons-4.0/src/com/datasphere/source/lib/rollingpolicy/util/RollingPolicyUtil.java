package com.datasphere.source.lib.rollingpolicy.util;

import java.util.*;
import com.datasphere.common.exc.*;

public class RollingPolicyUtil
{
    public static Map<String, Object> mapPolicyValueToRollOverPolicyName(final String uploadOrRolloverPolicy) throws AdapterException {
        final Map<String, Object> parsedPolicyMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        boolean rolloverPolicySet = false;
        final StringTokenizer tokenizer = new StringTokenizer(uploadOrRolloverPolicy, ",");
        while (tokenizer.hasMoreTokens()) {
            final String token = tokenizer.nextToken();
            String key = null;
            String value = null;
            if (token.contains(":")) {
                key = token.substring(0, token.indexOf(58));
                value = token.substring(token.indexOf(58) + 1, token.length());
                if (key.equalsIgnoreCase("rotationinterval")) {
                    key = "interval";
                }
            }
            else {
                if (!token.toLowerCase().contains("policy")) {
                    throw new AdapterException("Invalid Policy value " + token);
                }
                key = "rolloverpolicy";
                value = token;
                rolloverPolicySet = true;
            }
            parsedPolicyMap.put(key.trim(), value.trim());
        }
        if (!rolloverPolicySet) {
            if (parsedPolicyMap.containsKey("eventcount") && parsedPolicyMap.containsKey("interval")) {
                parsedPolicyMap.put("rolloverpolicy", "EventCountAndTimeRollingPolicy");
            }
            else if (parsedPolicyMap.containsKey("eventcount")) {
                parsedPolicyMap.put("rolloverpolicy", "EventCountRollingPolicy");
            }
            else if (parsedPolicyMap.containsKey("interval")) {
                parsedPolicyMap.put("rolloverpolicy", "TimeIntervalRollingPolicy");
            }
            else if (parsedPolicyMap.containsKey("filesize")) {
                parsedPolicyMap.put("rolloverpolicy", "FileSizeRollingPolicy");
            }
            else if (parsedPolicyMap.get("rolloverpolicy") == null || !((String)parsedPolicyMap.get("rolloverpolicy")).equalsIgnoreCase("DefaultRollingPolicy")) {
                throw new IllegalArgumentException("Invalid policy value passed :" + uploadOrRolloverPolicy);
            }
        }
        return parsedPolicyMap;
    }
}
