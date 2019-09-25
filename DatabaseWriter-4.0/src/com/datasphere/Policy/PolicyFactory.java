package com.datasphere.Policy;

import com.datasphere.source.lib.prop.*;
import com.datasphere.intf.*;

import java.util.*;

public class PolicyFactory
{
    public static String PRESERVE_SOURCE_TXN_BOUNARY;
    public static String BATCH_POLICY;
    public static String COMMIT_POLICY;
    public static String DISALED_BATCH_POLICY;
    public static String DEFAULT_BATCH_POLICY;
    public static String DEFAULT_COMMIT_POLICY;
    
    public static Policy createPolicy(final Property prop, final DBInterface dbImpl, final PolicyCallback callback) {
        final boolean preserveSrcTxnBoundary = prop.getBoolean(PolicyFactory.PRESERVE_SOURCE_TXN_BOUNARY, false);
        Property localProp = prop;
        if (preserveSrcTxnBoundary) {
            final Map<String, Object> propMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
            propMap.putAll(prop.getMap());
            propMap.remove(PolicyFactory.BATCH_POLICY);
            propMap.remove(PolicyFactory.COMMIT_POLICY);
            propMap.put(PolicyFactory.BATCH_POLICY, PolicyFactory.DISALED_BATCH_POLICY);
            localProp = new Property((Map)propMap);
        }
        final Property commitPolicyProp = getPolicyProperties(localProp, PolicyFactory.COMMIT_POLICY);
        final Property batchPolicyProp = getPolicyProperties(localProp, PolicyFactory.BATCH_POLICY);
        Policy policy = null;
        policy = ((commitPolicyProp != null) ? ((policy != null) ? new CommitPolicy(batchPolicyProp, policy) : new CommitPolicy(commitPolicyProp, dbImpl, callback)) : null);
        policy = ((batchPolicyProp != null) ? ((policy != null) ? new BatchPolicy(batchPolicyProp, policy) : new BatchPolicy(batchPolicyProp, dbImpl, callback)) : null);
        return policy;
    }
    
    public static Property getPolicyProperties(final Property prop, final String propName) {
        final String propValue = prop.getString(propName, (String)null);
        if (propValue != null) {
            final Map<String, Object> propertyMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
            final String[] split;
            final String[] propElements = split = propValue.split(",");
            for (final String propEntry : split) {
                final String[] entryElements = propEntry.split(":");
                if (entryElements.length > 1) {
                    propertyMap.put(entryElements[0], entryElements[1]);
                }
                else {
                    propertyMap.put(entryElements[0], null);
                }
            }
            return new Property((Map)propertyMap);
        }
        return null;
    }
    
    static {
        PolicyFactory.PRESERVE_SOURCE_TXN_BOUNARY = "PreserveSourceTransactionBoundary";
        PolicyFactory.BATCH_POLICY = "BatchPolicy";
        PolicyFactory.COMMIT_POLICY = "CommitPolicy";
        PolicyFactory.DISALED_BATCH_POLICY = "EventCount:1,Interval:0";
        PolicyFactory.DEFAULT_BATCH_POLICY = "EventCount:1000,Interval:60";
        PolicyFactory.DEFAULT_COMMIT_POLICY = "EventCount:1000,Interval:60";
    }
}
