package com.datasphere.pool;

import org.apache.log4j.*;

import com.datasphere.runtime.utils.*;

import java.util.*;

public class RetryPolicy
{
    public static final Logger logger;
    private int maxRetries;
    private int retryInterval;
    private int timeOut;
    
    public RetryPolicy() {
        this(3, 30, 30);
    }
    
    public RetryPolicy(final int maxRetries, final int retryInterval, final int timeOut) {
        this(3, 30, 30, TIMEUNIT.SEC);
    }
    
    public RetryPolicy(final int maxRetries, final int retryInterval, final int timeOut, final TIMEUNIT tu) {
        this.maxRetries = 3;
        this.retryInterval = 30;
        this.timeOut = 30;
        if (tu == TIMEUNIT.SEC) {
            this.retryInterval = retryInterval;
            this.timeOut = timeOut;
        }
        else if (tu == TIMEUNIT.MIN) {
            this.retryInterval = retryInterval * 60;
            this.timeOut = timeOut * 60;
        }
        this.maxRetries = maxRetries;
    }
    
    public static RetryPolicy get(final String policy) {
        if (policy == null || policy.isEmpty()) {
            return new RetryPolicy();
        }
        final Map<String, Object> connectionRetryPolicyMap = StringUtils.getKeyValuePairs(policy, ",", "=");
        int maxRetries = 3;
        int retryInterval = 30;
        int timeOut = 30;
        if (connectionRetryPolicyMap.get("maxRetries") != null) {
            try {
                maxRetries = StringUtils.getInt(connectionRetryPolicyMap, "maxRetries");
            }
            catch (Exception ex) {
                RetryPolicy.logger.warn((Object)"wrongly formatted maxRetries value provided. so setting 3 as default value.");
            }
            if (maxRetries < 1) {
                maxRetries = 0;
            }
        }
        if (connectionRetryPolicyMap.get("retryInterval") != null) {
            try {
                retryInterval = StringUtils.getInt(connectionRetryPolicyMap, "retryInterval");
            }
            catch (Exception ex) {
                RetryPolicy.logger.warn((Object)"wrongly formatted retryInterval value provided. so setting 30 seconds as default value.");
            }
            if (retryInterval < 1) {
                retryInterval = 0;
            }
        }
        if (connectionRetryPolicyMap.get("timeOut") != null) {
            try {
                timeOut = StringUtils.getInt(connectionRetryPolicyMap, "timeOut");
            }
            catch (Exception ex) {
                RetryPolicy.logger.warn((Object)"wrongly formatted timeOut value provided. so setting 30 seconds as default value.");
            }
            if (timeOut < 1) {
                timeOut = 0;
            }
        }
        return new RetryPolicy(maxRetries, retryInterval, timeOut);
    }
    
    public int getMaxRetries() {
        return this.maxRetries;
    }
    
    public void setMaxRetries(final int maxRetries) {
        this.maxRetries = maxRetries;
    }
    
    public int getRetryInterval() {
        return this.retryInterval;
    }
    
    public void setRetryInterval(final int retryInterval) {
        this.retryInterval = retryInterval;
    }
    
    public int getTimeOut() {
        return this.timeOut;
    }
    
    public void setTimeOut(final int timeOut) {
        this.timeOut = timeOut;
    }
    
    @Override
    public String toString() {
        return "Retry policy : maxRetries: " + this.maxRetries + ", retryInterval: " + this.retryInterval + " , timeOut: " + this.timeOut;
    }
    
    static {
        logger = Logger.getLogger((Class)RetryPolicy.class);
    }
    
    public enum TIMEUNIT
    {
        SEC, 
        MIN;
    }
}
