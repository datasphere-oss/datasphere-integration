package com.datasphere.recovery;

import java.io.*;
import com.fasterxml.jackson.annotation.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
public abstract class SourcePosition implements Serializable, Comparable<SourcePosition>
{
    private static final long serialVersionUID = 1657905561025367794L;
    
    @Override
    public abstract int compareTo(final SourcePosition p0);
    
    public String toHumanReadableString() {
        return this.toString();
    }
}
