package com.datasphere.source.lib.utils;

import java.util.*;
import org.apache.log4j.*;
import java.nio.file.attribute.*;
import java.io.*;
import java.nio.file.*;
import com.datasphere.common.exc.*;

public class DefaultFileComparator implements Comparator<Object>
{
    private Logger logger;
    
    public DefaultFileComparator() {
        this.logger = Logger.getLogger((Class)DefaultFileComparator.class);
    }
    
    @Override
    public int compare(final Object file1, final Object file2) {
        final int diff = (int)(this.getFileCreationTime(file1) - this.getFileCreationTime(file2));
        return diff;
    }
    
    public long getFileCreationTime(final Object file) {
        long creationTime = 0L;
        final Path path = ((File)file).toPath();
        try {
            final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, new LinkOption[0]);
            creationTime = attributes.creationTime().toMillis();
        }
        catch (IOException e) {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)"Couldn't compare file creation time");
            }
        }
        return creationTime;
    }
    
    public String getFileName(final Object file) throws AdapterException {
        return ((File)file).getName();
    }
}
