package com.datasphere.hdstore;

import com.datasphere.recovery.*;

public interface CheckpointManager
{
    Position get(final String p0);
    
    void add(final String p0, final HD p1, final Position p2);
    
    void start(final String p0);
    
    void flush(final HDStore p0);
    
    void remove(final String p0);
    
    void removeInvalidHDs(final String p0);
    
    void closeHDStore(final String p0);
    
    void writeBlankCheckpoint(final String p0);
}
