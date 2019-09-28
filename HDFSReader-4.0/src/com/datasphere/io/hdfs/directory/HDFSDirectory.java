package com.datasphere.io.hdfs.directory;

import com.datasphere.source.lib.directory.*;
import com.datasphere.source.lib.prop.*;
import java.util.*;
import com.datasphere.common.exc.*;
import java.io.*;
import com.datasphere.common.errors.*;
import com.datasphere.common.errors.Error;

import org.apache.hadoop.fs.*;
import java.util.concurrent.*;

public final class HDFSDirectory extends Directory
{
    int pollInterval;
    FileSystem hadoopFileSystem;
    String hadoopUri;
    Map<String, Object> fileMap;
    long defaultBlockSize;
    
    public HDFSDirectory(final Property prop, final FileSystem hadoopFileSystem, final Comparator<Object> comparator) throws AdapterException, IOException, InterruptedException {
        super((Comparator)comparator);
        this.fileMap = new HashMap<String, Object>();
        this.hadoopFileSystem = hadoopFileSystem;
        this.hadoopUri = prop.hadoopUrl + prop.wildcard;
        this.initializeFileList();
        this.pollInterval = 500;
        this.defaultBlockSize = hadoopFileSystem.getConf().getLong("dfs.blocksize", 0L);
        (this.WatcherThread = new Thread(new HDFSWatcher(hadoopFileSystem, prop))).setName("HDFSWatcherThread" + HDFSDirectory.threadCount++);
        this.WatcherThread.start();
    }
    
    private void initializeFileList() throws AdapterException {
        try {
            final FileStatus[] files = this.hadoopFileSystem.globStatus(new Path(this.hadoopUri));
            if (files != null) {
                for (final FileStatus file : files) {
                    if (file.isFile()) {
                        final String fileName = file.getPath().getName();
                        this.fileMap.put(fileName, file.getPath());
                        this.fileList.add(file.getPath());
                    }
                }
            }
        }
        catch (IllegalArgumentException | IOException e) {
            final AdapterException exp = new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
            throw exp;
        }
    }
    
    protected String getFileName(final Object file) {
        return this.fileMap.get(((Path)file).getName()).toString();
    }
    
    private class HDFSWatcher implements Runnable
    {
        FileSystem fsToBeWatched;
        
        public HDFSWatcher(final FileSystem fsToBeWatched, final Property prop) {
            this.fsToBeWatched = fsToBeWatched;
        }
        
        @Override
        public void run() {
            final Path fileNameToBePolled = new Path(HDFSDirectory.this.hadoopUri);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final FileStatus[] files = this.fsToBeWatched.globStatus(fileNameToBePolled);
                    if (files != null) {
                        for (final FileStatus file : files) {
                            if (file.isFile()) {
                                final String fileName = file.getPath().getName();
                                if (!HDFSDirectory.this.fileMap.containsKey(fileName)) {
                                    HDFSDirectory.this.fileMap.put(fileName, file.getPath());
                                    HDFSDirectory.this.fileList.add(file.getPath());
                                    if (Directory.logger.isDebugEnabled()) {
                                        Directory.logger.debug((Object)("New file is added in the HDFS " + fileName));
                                    }
                                }
                            }
                        }
                    }
                    Thread.sleep(HDFSDirectory.this.pollInterval);
                    continue;
                }
                catch (InterruptedException ie) {
                    return;
                }
                catch (IllegalArgumentException | IOException ex2) {
                    Directory.logger.debug((Object)"Got exception while getting file list");
                    continue;
                }
            }
        }
    }
}
