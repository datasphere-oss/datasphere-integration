package com.datasphere.runtime;

import org.apache.log4j.*;

import com.datasphere.runtime.utils.*;

import java.util.*;

public class RecordKey implements Comparable<RecordKey>
{
    private static final Logger logger;
    protected Object[] fields;
    public Object singleField;
    protected int hashCode;
    public static final RecordKey emptyKey;
    
    public RecordKey() {
        this(null);
    }
    
    public RecordKey(final Object[] fields) {
        if (fields != null && fields.length == 1) {
            this.singleField = fields[0];
            this.fields = null;
        }
        else {
            this.singleField = null;
            this.fields = fields;
        }
        this.hashCode = 0;
    }
    
    @Override
    public String toString() {
        if (this.singleField != null) {
            return this.singleField.toString();
        }
        if (this.isNull()) {
            return "null";
        }
        return StringUtils.join(this.fields);
    }
    
    public String toPartitionKey() {
        if (this.fields == null && this.singleField != null) {
            return String.valueOf(this.singleField);
        }
        if (this.isNull()) {
            return "null";
        }
        return StringUtils.join(this.fields, "#");
    }
    
    @Override
    public int hashCode() {
        if (this.hashCode == 0) {
            if (this.singleField != null) {
                this.hashCode = this.singleField.hashCode();
            }
            else {
                this.hashCode = Arrays.hashCode(this.fields);
            }
        }
        return this.hashCode;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecordKey)) {
            return false;
        }
        final RecordKey other = (RecordKey)o;
        if (this.singleField != null) {
            return this.singleField.equals(other.singleField);
        }
        return Arrays.equals(this.fields, other.fields);
    }
    
    @Override
    public int compareTo(final RecordKey o) {
        assert this.fields.length == o.fields.length;
        if (this.singleField != null) {
            return compare(this.singleField, o.singleField);
        }
        for (int i = 0; i < this.fields.length; ++i) {
            final int r = compare(this.fields[i], o.fields[i]);
            if (r != 0) {
                return r;
            }
        }
        return 0;
    }
    
    public boolean isEmpty() {
        return this.fields.length == 0;
    }
    
    public boolean isNull() {
        if (this.fields == null && this.singleField == null) {
            return true;
        }
        if (this.fields != null) {
            for (int i = 0; i < this.fields.length; ++i) {
                if (this.fields[i] == null) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private static int compare(final Object a, final Object b) {
        if (a == null) {
            return (b != null) ? 1 : 0;
        }
        if (b == null) {
            return (a == null) ? 0 : -1;
        }
        return ((Comparable)a).compareTo(b);
    }
    
    public static String getObjArrayKeyFactory() {
        return RecordKey.class.getName() + ".createKeyFromObjArray";
    }
    
    public static RecordKey createKeyFromObjArray(final Object[] args) {
        return new RecordKey(args);
    }
    
    public static RecordKey createRecordKey(final Object args) {
        return new CacheKey(args);
    }
    
    public static RecordKey cacheRecordKeyCreator(final Object[] arg) {
        return new CacheKey(arg);
    }
    
    public static RecordKey createKey(final Object... args) {
        return new RecordKey(args);
    }
    
    static {
        logger = Logger.getLogger(RecordKey.class.getName());
        emptyKey = createKey(new Object[0]);
    }
}
