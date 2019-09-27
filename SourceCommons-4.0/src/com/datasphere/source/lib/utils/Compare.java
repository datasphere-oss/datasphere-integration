package com.datasphere.source.lib.utils;

import org.apache.log4j.*;
import java.nio.file.attribute.*;
import java.io.*;
import java.nio.file.*;

public class Compare implements Comparable<Object>
{
    public long time;
    public File file;
    private Logger logger;
    
    public Compare(final File file) {
        this.logger = Logger.getLogger((Class)Compare.class);
        this.file = file;
        this.time = this.getFileCreationTime(file);
    }
    
    private long getFileCreationTime(final File file) {
        final Path path = file.toPath();
        long creationTime = 0L;
        try {
            final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, new LinkOption[0]);
            creationTime = attributes.creationTime().toMillis();
        }
        catch (IOException e) {
            this.logger.error((Object)e);
        }
        return creationTime;
    }
    
    @Override
    public int compareTo(final Object comp) {
        final long u = ((Compare)comp).time;
        return (this.time < u) ? -1 : ((this.time == u) ? 0 : 1);
    }
}
