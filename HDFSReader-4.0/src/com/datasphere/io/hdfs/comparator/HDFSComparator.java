package com.datasphere.io.hdfs.comparator;

import com.datasphere.source.lib.utils.*;
import org.apache.log4j.*;
import java.io.*;
import org.apache.hadoop.fs.*;
import com.datasphere.common.exc.*;

public class HDFSComparator extends DefaultFileComparator
{
    private FileSystem hadoopFileSystem;
    private Logger logger;
    
    public HDFSComparator(final FileSystem hadoopFileSystem) {
        this.logger = Logger.getLogger((Class)HDFSComparator.class);
        this.hadoopFileSystem = hadoopFileSystem;
    }
    
    public long getFileCreationTime(final Object file) {
        long creationTime = 0L;
        final Path path = (Path)file;
        try {
            final FileStatus fs = this.hadoopFileSystem.getFileStatus(path);
            creationTime = fs.getModificationTime();
        }
        catch (IOException e) {
            this.logger.debug((Object)("Couldn't get file creation/modification time for file " + path.getName()));
        }
        return creationTime;
    }
    
    public String getFileName(final Object file) throws AdapterException {
        return ((Path)file).getName();
    }
}

