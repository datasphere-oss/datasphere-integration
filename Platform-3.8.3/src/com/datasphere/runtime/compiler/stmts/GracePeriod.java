package com.datasphere.runtime.compiler.stmts;

import java.io.*;
import com.datasphere.runtime.*;

public class GracePeriod implements Serializable
{
    private static final long serialVersionUID = 6064432984395345052L;
    public final Interval sortTimeInterval;
    public final String fieldName;
    
    public GracePeriod(final Interval sortInterval, final String fieldName) {
        this.sortTimeInterval = sortInterval;
        this.fieldName = fieldName;
    }
}
