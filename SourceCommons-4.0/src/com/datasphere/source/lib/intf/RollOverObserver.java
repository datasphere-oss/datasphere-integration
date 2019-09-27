package com.datasphere.source.lib.intf;

import java.io.*;

public interface RollOverObserver
{
    void preRollover() throws IOException;
    
    void postRollover(final String p0) throws IOException;
    
    void notifyOnInitialOpen() throws Exception;
}
