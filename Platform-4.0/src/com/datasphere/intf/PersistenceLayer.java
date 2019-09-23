package com.datasphere.intf;

import com.datasphere.recovery.*;
import java.util.*;
import com.datasphere.hd.*;
import com.datasphere.persistence.*;

public interface PersistenceLayer
{
    void init();
    
    void init(final String p0);
    
    int delete(final Object p0);
    
    Range[] persist(final Object p0);
    
    void merge(final Object p0);
    
    void setStoreName(final String p0, final String p1);
    
    Object get(final Class<?> p0, final Object p1);
    
    List<?> runQuery(final String p0, final Map<String, Object> p1, final Integer p2);
    
    Object runNativeQuery(final String p0);
    
    int executeUpdate(final String p0, final Map<String, Object> p1);
    
    void close();
    
    Position getWSPosition(final String p0, final String p1);
    
    boolean clearWSPosition(final String p0, final String p1);
    
     <T extends HD> Iterable<T> getResults(final Class<T> p0, final Map<String, Object> p1, final Set<HDKey> p2);
    
     <T extends HD> Iterable<T> getResults(final Class<T> p0, final String p1, final Map<String, Object> p2);
    
    HStore getHDStore();
    
    void setHDStore(final HStore p0);
    
    public static class Range
    {
        private final int start;
        private final int end;
        private boolean isSuccessful;
        
        public Range(final int start, final int end) {
            this.isSuccessful = false;
            this.start = start;
            this.end = end;
        }
        
        public int getStart() {
            return this.start;
        }
        
        public int getEnd() {
            return this.end;
        }
        
        public boolean isSuccessful() {
            return this.isSuccessful;
        }
        
        public void setSuccessful(final boolean isSuccessful) {
            this.isSuccessful = isSuccessful;
        }
    }
}
