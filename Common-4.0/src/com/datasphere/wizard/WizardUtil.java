package com.datasphere.wizard;

import java.util.*;

public class WizardUtil
{
    public static final String KEY_QUERY = "query";
    // 验证向导请求
    public static boolean validateRequest(final String[] keys, final Map<String, Object> request) throws IllegalArgumentException {
        final boolean result = true;
        for (final String key : keys) {
            final boolean contains = request.containsKey(key);
            if (!contains) {
                throw new IllegalArgumentException(key + " is expected in the request arguments");
            }
        }
        return result;
    }
}
