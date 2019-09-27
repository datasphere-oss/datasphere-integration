package com.datasphere.source.lib.directory;

import org.apache.log4j.*;
import java.util.concurrent.*;
import java.util.*;

import com.datasphere.common.exc.*;
import com.datasphere.source.lib.utils.*;

import java.io.*;

public class Directory
{
    public static Logger logger;
    protected PriorityBlockingQueue<Object> fileList;
    Object currentFile;
    protected Thread WatcherThread;
    protected static int threadCount;
    Comparator<Object> comparator;
    
    public Directory(final Comparator<Object> comparator) {
        this.currentFile = null;
        this.comparator = comparator;
        this.init();
    }
    
    private void init() {
        if (this.comparator == null) {
            this.comparator = new DefaultFileComparator();
        }
        this.fileList = new PriorityBlockingQueue<Object>(10, this.comparator);
    }
    
    public Object getCurrentFile() {
        return this.currentFile;
    }
    
    public int getFileListSize() {
        return this.fileList.size();
    }
    
    public Object getNextFile() {
        if (!this.fileList.isEmpty()) {
            return this.currentFile = this.fileList.poll();
        }
        return null;
    }
    
    public Object getFile(final String fileName) throws AdapterException {
        final Object[] list = this.fileList.toArray();
        String name = null;
        for (int idx = 0; idx < list.length; ++idx) {
            final Object file = list[idx];
            name = this.getFileName(file);
            if (name.equals(fileName)) {
                this.currentFile = list[idx];
                this.fileList.remove(this.currentFile);
                for (int itr = 0; itr < idx; ++itr) {
                    this.fileList.remove();
                }
                return this.currentFile;
            }
        }
        return null;
    }
    
    protected String getFileName(final Object file) {
        return ((File)file).getName();
    }
    
    public boolean isLastFile() {
        final int filesCount = this.fileList.size();
        return filesCount == 0;
    }
    
    public void stopThread() {
        this.WatcherThread.interrupt();
    }
    
    static {
        Directory.logger = Logger.getLogger((Class)Directory.class);
    }
}
