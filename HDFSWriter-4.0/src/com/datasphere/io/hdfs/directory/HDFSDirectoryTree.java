package com.datasphere.io.hdfs.directory;

import com.datasphere.io.dynamicdirectory.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import java.io.*;

public class HDFSDirectoryTree extends DirectoryTree
{
    private FileSystem hadoopFileSystem;
    
    public HDFSDirectoryTree(final Map<String, Object> properties, final FileSystem hadoopFileSystem) {
        super((Map)properties, (String)null);
        this.hadoopFileSystem = hadoopFileSystem;
        this.separator = "/";
    }
    
    protected boolean createDirectory(final String directoryName) throws IOException {
        final Path directory = new Path(directoryName);
        this.directoryCache.put(directoryName, directory);
        if (this.hadoopFileSystem.mkdirs(directory)) {
            return true;
        }
        if (this.hadoopFileSystem.exists(directory)) {
            return true;
        }
        throw new IOException("Failure in creating directory " + directory.getName() + " in HDFS");
    }
}
