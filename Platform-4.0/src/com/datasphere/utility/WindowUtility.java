package com.datasphere.utility;

import java.util.*;

public class WindowUtility
{
    public static void validateIntervalPolicies(final boolean isJumping, final Map<String, Object> policy, final Map<String, Object> slidePolicy) {
        final boolean valid = validateCountBasedPolicy(isJumping, policy, slidePolicy) || validateTimeBasedPolicy(isJumping, policy, slidePolicy) || validateAttributeBasedPolicy(isJumping, policy, slidePolicy) || validateCountTimeBasedPolicy(isJumping, policy, slidePolicy) || validateAttributeTimeBasedPolicy(isJumping, policy, slidePolicy);
        if (!valid) {
            throw new RuntimeException(prepareMessage(isJumping, policy, slidePolicy));
        }
    }
    
    public static boolean validateCountBasedPolicy(final boolean isJumping, final Map<String, Object> policy, final Map<String, Object> slidePolicy) {
        return (!isJumping && policy.get("count") != null && policy.get("time") == null && policy.get("onField") == null && policy.get("timeout") == null && (slidePolicy == null || slidePolicy.isEmpty())) || (isJumping && policy.get("count") != null && policy.get("time") == null && policy.get("onField") == null && policy.get("timeout") == null && (slidePolicy == null || slidePolicy.isEmpty())) || (!isJumping && policy.get("count") != null && policy.get("time") == null && policy.get("onField") == null && policy.get("timeout") == null && slidePolicy != null && slidePolicy.get("count") != null && slidePolicy.get("time") == null);
    }
    
    public static boolean validateTimeBasedPolicy(final boolean isJumping, final Map<String, Object> policy, final Map<String, Object> slidePolicy) {
        return (!isJumping && policy.get("time") != null && policy.get("count") == null && policy.get("onField") == null && policy.get("timeout") == null && (slidePolicy == null || slidePolicy.isEmpty())) || (isJumping && policy.get("time") != null && policy.get("count") == null && policy.get("onField") == null && policy.get("timeout") == null && (slidePolicy == null || slidePolicy.isEmpty())) || (!isJumping && policy.get("time") != null && policy.get("count") == null && policy.get("onField") == null && policy.get("timeout") == null && slidePolicy != null && slidePolicy.get("time") != null && slidePolicy.get("count") == null);
    }
    
    public static boolean validateAttributeBasedPolicy(final boolean isJumping, final Map<String, Object> policy, final Map<String, Object> slidePolicy) {
        return (!isJumping && policy.get("time") != null && policy.get("count") == null && policy.get("onField") != null && policy.get("timeout") == null && (slidePolicy == null || slidePolicy.isEmpty())) || (isJumping && policy.get("time") != null && policy.get("count") == null && policy.get("onField") != null && policy.get("timeout") == null && (slidePolicy == null || slidePolicy.isEmpty())) || (!isJumping && policy.get("time") != null && policy.get("count") == null && policy.get("onField") != null && policy.get("timeout") == null && slidePolicy != null && slidePolicy.get("time") != null && slidePolicy.get("count") == null);
    }
    
    public static boolean validateCountTimeBasedPolicy(final boolean isJumping, final Map<String, Object> policy, final Map<String, Object> slidePolicy) {
        return (!isJumping && policy.get("count") != null && policy.get("timeout") != null && policy.get("onField") == null && policy.get("time") == null && (slidePolicy == null || slidePolicy.isEmpty())) || (isJumping && policy.get("count") != null && policy.get("timeout") != null && policy.get("onField") == null && policy.get("time") == null && (slidePolicy == null || slidePolicy.isEmpty())) || (!isJumping && policy.get("count") != null && policy.get("timeout") != null && policy.get("onField") == null && policy.get("time") == null && slidePolicy != null && slidePolicy.get("count") != null && slidePolicy.get("time") == null);
    }
    
    public static boolean validateAttributeTimeBasedPolicy(final boolean isJumping, final Map<String, Object> policy, final Map<String, Object> slidePolicy) {
        return (!isJumping && policy.get("time") != null && policy.get("count") == null && policy.get("onField") != null && policy.get("timeout") != null && (slidePolicy == null || slidePolicy.isEmpty())) || (isJumping && policy.get("time") != null && policy.get("count") == null && policy.get("onField") != null && policy.get("timeout") != null && (slidePolicy == null || slidePolicy.isEmpty())) || (!isJumping && policy.get("time") != null && policy.get("count") == null && policy.get("onField") != null && policy.get("timeout") != null && slidePolicy != null && slidePolicy.get("time") != null && slidePolicy.get("count") == null);
    }
    
    private static String prepareMessage(final boolean isJumping, final Map<String, Object> policy, final Map<String, Object> slidePolicy) {
        final StringBuilder builder = new StringBuilder("Invalid policy specified. ");
        builder.append("Jumping window: ").append(isJumping).append("\n");
        builder.append("Window policy: ").append(policy).append("\n");
        builder.append("Sliding policy: ").append(slidePolicy).append("\n");
        return builder.toString();
    }
}
