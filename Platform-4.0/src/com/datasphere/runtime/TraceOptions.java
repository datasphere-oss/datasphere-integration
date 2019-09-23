package com.datasphere.runtime;

import java.io.*;
import java.util.*;

public class TraceOptions implements Serializable
{
    private static final long serialVersionUID = 6782882324928737147L;
    public int traceFlags;
    public int limit;
    public final String traceFilename;
    public final String traceFilePath;
    private static final Map<String, PrintStream> cachedOpenTraceStreams;
    
    public TraceOptions() {
        this.traceFlags = 0;
        this.traceFilename = null;
        this.traceFilePath = null;
        this.limit = -1;
    }
    
    public TraceOptions(final int traceFlags, final String traceFilename, final String traceFilePath) {
        this.traceFlags = traceFlags;
        this.traceFilename = traceFilename;
        this.traceFilePath = traceFilePath;
        this.limit = -1;
    }
    
    public TraceOptions(final int traceFlags, final String traceFilename, final String traceFilePath, final int limitVal) {
        this.traceFlags = traceFlags;
        this.traceFilename = traceFilename;
        this.traceFilePath = traceFilePath;
        this.limit = limitVal;
    }
    
    @Override
    public String toString() {
        return "TraceOptions(" + this.traceFlags + "," + this.traceFilename + ")";
    }
    
    public static PrintStream getTraceStream(final TraceOptions opt) {
        final String name = opt.traceFilename;
        if (name == null) {
            return System.out;
        }
        PrintStream res = TraceOptions.cachedOpenTraceStreams.get(name);
        if (res == null) {
            try {
                res = new PrintStream(new FileOutputStream(name), true);
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
                return System.out;
            }
            TraceOptions.cachedOpenTraceStreams.put(name, res);
        }
        return res;
    }
    
    static {
        cachedOpenTraceStreams = new LinkedHashMap<String, PrintStream>(4, 0.75f, true) {
            private static final long serialVersionUID = 1L;
            
            @Override
            protected boolean removeEldestEntry(final Map.Entry<String, PrintStream> eldest) {
                return this.size() > 4;
            }
        };
    }
}
