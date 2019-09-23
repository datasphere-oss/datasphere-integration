package com.datasphere.runtime;

import org.apache.log4j.*;
import java.util.*;
import java.util.concurrent.locks.*;

public class LocalLockProvider
{
    private static Logger logger;
    private static final Map<String, Lock> lockMap;
    private static Lock lockForLockMap;
    
    public static Lock getLock(final Key key) {
        LocalLockProvider.lockForLockMap.lock();
        try {
            if (LocalLockProvider.lockMap.get(key.toString()) == null) {
                LocalLockProvider.lockMap.put(key.toString(), new CustomLock());
                if (LocalLockProvider.logger.isDebugEnabled()) {
                    LocalLockProvider.logger.debug((Object)("created lock for key : " + key));
                }
            }
        }
        finally {
            LocalLockProvider.lockForLockMap.unlock();
        }
        return LocalLockProvider.lockMap.get(key.toString());
    }
    
    public static void removeLock(final Key key) {
        LocalLockProvider.lockForLockMap.lock();
        try {
            if (LocalLockProvider.lockMap.get(key.toString()) != null) {
                LocalLockProvider.lockMap.remove(key.toString());
                if (LocalLockProvider.logger.isDebugEnabled()) {
                    LocalLockProvider.logger.debug((Object)("removed lock for key : " + key));
                }
            }
        }
        finally {
            LocalLockProvider.lockForLockMap.unlock();
        }
    }
    
    static {
        LocalLockProvider.logger = Logger.getLogger((Class)LocalLockProvider.class);
        lockMap = new HashMap<String, Lock>();
        LocalLockProvider.lockForLockMap = new ReentrantLock();
    }
    
    private static class CustomLock extends ReentrantLock
    {
        private static final long serialVersionUID = -1486927071175970257L;
        
        @Override
        public void lock() {
            super.lock();
        }
        
        @Override
        public void unlock() {
            super.unlock();
        }
    }
    
    public static class Key
    {
        String part1;
        String part2;
        
        public Key(final String part1, final String part2) {
            this.part1 = part1;
            this.part2 = part2;
        }
        
        public String get() {
            return this.part1 + "-" + this.part2;
        }
        
        @Override
        public String toString() {
            return this.get();
        }
    }
}
