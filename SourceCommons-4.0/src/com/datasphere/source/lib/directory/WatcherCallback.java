package com.datasphere.source.lib.directory;

import java.io.*;

public interface WatcherCallback
{
    void onFileCreate(final File p0);
    
    void onFileDelete(final File p0);
    
    void onFileModify(final File p0);
    
    void onFileDelete(final String p0, final String p1);
}
