package com.datasphere.io.hdfs.directory;

import org.apache.hadoop.fs.*;
import com.datasphere.source.lib.rollingpolicy.property.*;
import com.datasphere.source.lib.intf.*;
import java.util.*;
import com.datasphere.io.dynamicdirectory.*;

public class HDFSDynamicDirectoryOutputStreamGenerator extends DynamicDirectoryOutputStreamGenerator
{
    private FileSystem hadoopFileSystem;
    
    public HDFSDynamicDirectoryOutputStreamGenerator(final RollOverProperty rollOverProperty, final RollOverObserver rollOverObserver, final FileSystem hadoopFileSystem) {
        super(rollOverObserver);
        this.hadoopFileSystem = hadoopFileSystem;
    }
    
    protected DirectoryTree createDirectoryTree(final Map<String, Object> properties) {
        return new HDFSDirectoryTree(properties, this.hadoopFileSystem);
    }
}
