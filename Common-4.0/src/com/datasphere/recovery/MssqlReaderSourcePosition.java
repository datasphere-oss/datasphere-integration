package com.datasphere.recovery;

import org.apache.log4j.*;
import javax.xml.bind.*;

public class MssqlReaderSourcePosition extends SourcePosition
{
    private static final long serialVersionUID = -6042045728463138984L;
    private static Logger logger;
    String currentTableName;
    String lsnString;
    private String lsnHexValue;
    
    public MssqlReaderSourcePosition(final String tableName, final String lsnString) {
        this.lsnString = null;
        this.lsnHexValue = null;
        this.currentTableName = tableName;
        this.lsnString = lsnString;
        this.lsnHexValue = DatatypeConverter.printHexBinary(lsnString.getBytes());
    }
    
    @Override
    public int compareTo(final SourcePosition arg0) {
        if (!(arg0 instanceof MssqlReaderSourcePosition)) {
            throw new IllegalArgumentException("Attempted to compare a MssqlReaderSourcePosition to a " + arg0.getClass().getSimpleName());
        }
        final MssqlReaderSourcePosition that = (MssqlReaderSourcePosition)arg0;
        if (!this.currentTableName.equals(that.currentTableName)) {
            throw new IllegalArgumentException("Attempted to compare dissimilar table positions");
        }
        final int res = this.lsnHexValue.compareTo(that.getHexValue());
        if (MssqlReaderSourcePosition.logger.isTraceEnabled()) {
            MssqlReaderSourcePosition.logger.trace((Object)("Table: " + this.currentTableName + ", this.compareto(that): " + res));
        }
        return res;
    }
    
    protected String getHexValue() {
        return this.lsnHexValue;
    }
    
    @Override
    public String toString() {
        return this.lsnString;
    }
    
    static {
        MssqlReaderSourcePosition.logger = Logger.getLogger((Class)MssqlReaderSourcePosition.class);
    }
}
