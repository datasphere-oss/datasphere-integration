package com.datasphere.runtime.utils;

import org.apache.log4j.*;

public class ThreadUtils
{
    private static Logger logger;
    
    public static int wait(final int waitTime) {
        return wait(0, 0, waitTime);
    }
    
    public static int wait(int attemptNum, final int maxRetries, final int waitTime) {
        ThreadUtils.logger.warn((Object)("Waiting " + waitTime + " seconds before next attempt....... " + attemptNum));
        boolean stillWait = true;
        final long t1 = System.currentTimeMillis();
        while (stillWait) {
            try {
                Thread.sleep(waitTime * 1000);
            }
            catch (InterruptedException iex) {
                stillWait = false;
                attemptNum = maxRetries;
            }
            final long t2 = System.currentTimeMillis();
            if (t2 - t1 > waitTime * 1000) {
                ThreadUtils.logger.warn((Object)"done waiting.");
                stillWait = false;
            }
        }
        return attemptNum;
    }
    
    static {
        ThreadUtils.logger = Logger.getLogger((Class)ThreadUtils.class);
    }
}
