package com.datasphere.runtime.compiler.stmts;

import java.io.*;

public class RecoveryDescription implements Serializable
{
    public static final String PARAM = "RecoveryDescription";
    public static final String SIGNALS_NAME = "StartupSignalsSetName";
    public static final String SIGNALS_PREFIX = "StartupSignals";
    public final int type;
    public final long interval;
    
    private RecoveryDescription() {
        this.type = 0;
        this.interval = 0L;
    }
    
    public RecoveryDescription(final int type, final long interval) {
        this.type = type;
        this.interval = interval;
    }
}
