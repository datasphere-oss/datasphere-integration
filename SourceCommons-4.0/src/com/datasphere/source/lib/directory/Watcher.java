package com.datasphere.source.lib.directory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.directory.util.DirectoryFilter;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.source.lib.utils.DefaultFileComparator;

public abstract class Watcher implements Runnable
{
    Logger logger;
    WatcherCallback callback;
    Thread watcherThread;
    PathMatcher matcher;
    Path dir;
    boolean stopCalled;
    Property prop;
    protected int maxFilesToFetch;
    boolean recoveryMode;
    boolean isMultiFileBank;
    protected boolean readSubDirectories;
    private DirectoryFilter directoryFilter;
    private WildcardFilter wildcardFilter;
    
    public Watcher(final Property prop, final WatcherCallback callback) throws IOException {
        this.logger = Logger.getLogger((Class)Watcher.class);
        this.maxFilesToFetch = -1;
        this.readSubDirectories = false;
        this.directoryFilter = null;
        this.wildcardFilter = null;
        this.callback = callback;
        this.matcher = FileSystems.getDefault().getPathMatcher("glob:" + prop.wildcard);
        this.recoveryMode = prop.getBoolean(Reader.RECOVERY_MODE, false);
        this.readSubDirectories = prop.getBoolean("includesubdirectories", false);
        this.directoryFilter = new DirectoryFilter();
        this.wildcardFilter = new WildcardFilter(prop.wildcard);
        final File directory = new File(prop.directory);
        if (!directory.exists()) {
            throw new IOException("Directory [" + prop.directory + "] is not present");
        }
        this.dir = Paths.get(prop.directory, new String[0]);
        this.prop = prop;
        this.isMultiFileBank = (callback instanceof MultiFileBank);
    }
    
    protected abstract void init() throws AdapterException;
    
    public void invalidateCacheFor(final String fileName) {
    }
    
    protected File[] getFiles() throws AdapterException {
        final PriorityBlockingQueue<File> fileList = new PriorityBlockingQueue<File>(10, new DefaultFileComparator());
        try {
            int fileCnt = 0;
            final DirectoryStream<Path> listOfPaths = Files.newDirectoryStream(this.dir, new WildcardFilter(this.prop.wildcard));
            for (final Path path : listOfPaths) {
                if (this.maxFilesToFetch > 0 && fileCnt >= this.maxFilesToFetch) {
                    break;
                }
                ++fileCnt;
                fileList.add(path.toFile());
            }
            listOfPaths.close();
            if (this.readSubDirectories) {
                fileList.addAll(this.getFilesFromSubDirectories(this.dir, new PriorityBlockingQueue<File>(10, new DefaultFileComparator())));
            }
        }
        catch (IOException e) {
            final String msg = "Got IOException while creating DirectoryStream for {" + this.dir.toString() + "}";
            this.logger.warn((Object)msg);
            throw new AdapterException(msg, (Throwable)e);
        }
        final ArrayList<File> tmpFileList = new ArrayList<File>();
        for (int fileCnt2 = fileList.size(), itr = 0; itr < fileCnt2; ++itr) {
            final File file = fileList.poll();
            if (file != null) {
                tmpFileList.add(file);
            }
        }
        return tmpFileList.toArray(new File[tmpFileList.size()]);
    }
    
    public void start() throws AdapterException {
        this.publishCurrentStatus();
        (this.watcherThread = new Thread(this)).setName("WatcherThread-" + this.prop.directory + ":" + this.prop.wildcard);
        this.watcherThread.start();
    }
    
    private void publishCurrentStatus() throws AdapterException {
        final File[] fileList = this.getFiles();
        if (this.recoveryMode || !this.prop.positionByEOF) {
            for (int itr = 0; itr < fileList.length; ++itr) {
                this.onFileCreate(fileList[itr]);
            }
        }
        else if (!this.isMultiFileBank && fileList.length > 0) {
            this.onFileCreate(fileList[fileList.length - 1]);
        }
    }
    
    public void stop() {
        this.stopCalled = true;
        if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)"Signaled wather-thread to stop");
        }
        this.watcherThread.interrupt();
    }
    
    protected boolean checkForWildcardMatch(final File file) {
        return this.matcher.matches(file.toPath().getFileName());
    }
    
    protected void onFileCreate(final File file) {
        if (this.checkForWildcardMatch(file)) {
            this.callback.onFileCreate(file);
        }
    }
    
    protected void onFileDelete(final File file) {
        if (this.checkForWildcardMatch(file)) {
            this.callback.onFileDelete(file);
        }
    }
    
    protected void onFileDelete(final String fileKey, final String fileName) {
        this.callback.onFileDelete(fileKey, fileName);
    }
    
    protected void onFileModify(final File file) {
        if (this.checkForWildcardMatch(file)) {
            this.callback.onFileModify(file);
        }
    }
    
    protected PriorityBlockingQueue<File> getFilesFromSubDirectories(final Path directory, final PriorityBlockingQueue<File> fileList) {
        try {
            final DirectoryStream<Path> listOfSubDirectories = Files.newDirectoryStream(directory, this.directoryFilter);
            for (final Path subDirectory : listOfSubDirectories) {
                final DirectoryStream<Path> filesInEachSubDirectory = Files.newDirectoryStream(subDirectory, this.wildcardFilter);
                for (final Path path : filesInEachSubDirectory) {
                    fileList.add(path.toFile());
                }
                filesInEachSubDirectory.close();
                this.getFilesFromSubDirectories(subDirectory, fileList);
            }
            listOfSubDirectories.close();
        }
        catch (IOException ioe) {
            return fileList;
        }
        return fileList;
    }
    
    class WildcardFilter implements DirectoryStream.Filter<Path>
    {
        PathMatcher matcher;
        
        public WildcardFilter(final String wildcard) {
            this.matcher = FileSystems.getDefault().getPathMatcher("glob:" + wildcard);
        }
        
        @Override
        public boolean accept(final Path entry) {
            return this.matcher.matches(entry.getFileName());
        }
    }
}
