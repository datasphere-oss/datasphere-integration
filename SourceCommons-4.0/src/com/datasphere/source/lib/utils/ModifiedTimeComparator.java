package com.datasphere.source.lib.utils;

import org.apache.log4j.*;
import java.nio.file.attribute.*;
import java.io.*;
import java.nio.file.*;

public class ModifiedTimeComparator extends DefaultFileComparator
{
    private Logger logger;
    
    public ModifiedTimeComparator() {
        this.logger = Logger.getLogger((Class)DefaultFileComparator.class);
    }
    
    @Override
    public long getFileCreationTime(final Object file) {
        long creationTime = 0L;
        if (file instanceof File) {
            final Path path = ((File)file).toPath();
            try {
                final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, new LinkOption[0]);
                creationTime = attributes.lastModifiedTime().toMillis();
            }
            catch (IOException e) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)"Couldn't compare file creation time");
                }
            }
        }
        else if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)"Unsupported object. Not going to compare file creation time");
        }
        return creationTime;
    }
}
