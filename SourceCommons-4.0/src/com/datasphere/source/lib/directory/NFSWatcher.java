package com.datasphere.source.lib.directory;

import org.apache.log4j.*;
import java.util.concurrent.*;
import java.nio.file.attribute.*;
import java.io.*;

import com.datasphere.source.lib.prop.*;
import com.datasphere.source.lib.utils.*;
import com.datasphere.common.exc.*;
import java.nio.file.*;
import java.util.*;

public class NFSWatcher extends Watcher
{
    Logger logger;
    public static int DEFAULT_MAX_FILE_CNT;
    PriorityBlockingQueue<File> fileList;
    private Path directory;
    int poolInterval;
    Map<String, File> keyToFileObjMap;
    Map<String, BasicFileAttributes> keytoFileAttrMap;
    Map<String, Long> creationTimeMap;
    Map<String, String> ignoreKeyMap;
    boolean positionByEOF;
    
    public NFSWatcher(final Property prop, final WatcherCallback callback) throws IOException {
        super(prop, callback);
        this.logger = Logger.getLogger((Class)NFSWatcher.class);
        this.maxFilesToFetch = 10000;
    }
    
    @Override
    protected void init() throws AdapterException {
        this.poolInterval = this.prop.getInt(Property.POLL_INTERVAL, 15);
        this.keyToFileObjMap = new HashMap<String, File>();
        this.keytoFileAttrMap = new HashMap<String, BasicFileAttributes>();
        this.creationTimeMap = new HashMap<String, Long>();
        this.fileList = new PriorityBlockingQueue<File>(10, new DefaultFileComparator());
        this.ignoreKeyMap = new HashMap<String, String>();
    }
    
    @Override
    public void stop() {
        if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)"Stopping NFSWatcher....");
        }
        super.stop();
    }
    
    @Override
    protected File[] getFiles() throws AdapterException {
        final File[] tmpList = super.getFiles();
        for (int itr = 0; itr < tmpList.length; ++itr) {
            try {
                final BasicFileAttributes attr = Files.readAttributes(tmpList[itr].toPath(), BasicFileAttributes.class, new LinkOption[0]);
                final String fileKey = attr.fileKey().toString();
                this.keyToFileObjMap.put(fileKey, tmpList[itr]);
                if (this.prop.positionByEOF) {
                    this.ignoreKeyMap.put(fileKey, fileKey);
                    this.keytoFileAttrMap.put(fileKey, attr);
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tmpList;
    }
    
    @Override
    public void run() {
        this.directory = Paths.get(this.prop.directory, new String[0]);
        final List<File> listOfFiles = new ArrayList<File>();
        final int sleepTime = this.poolInterval * 100;
        final Map<String, File> deletedFileMap = new TreeMap<String, File>();
        String topFileName = null;
        if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)("Will Ignore the first event for the following file keys {" + this.ignoreKeyMap.toString() + "}"));
        }
        while (!this.stopCalled) {
            try {
                int fileCnt = 0;
                boolean foundMatchedFile = false;
                if (this.readSubDirectories) {
                    listOfFiles.addAll(this.getFilesFromSubDirectories(this.dir, new PriorityBlockingQueue<File>(10, new DefaultFileComparator())));
                }
                final DirectoryStream<Path> filesInRootDirectory = Files.newDirectoryStream(this.directory, new WildcardFilter(this.prop.wildcard));
                for (final Path fileinRootDirectory : filesInRootDirectory) {
                    listOfFiles.add(fileinRootDirectory.toFile());
                }
                filesInRootDirectory.close();
                this.fileList.clear();
                deletedFileMap.clear();
                deletedFileMap.putAll(this.keyToFileObjMap);
                for (final File fileInList : listOfFiles) {
                    foundMatchedFile = true;
                    this.fileList.add(fileInList);
                }
                listOfFiles.clear();
                File file;
                while ((file = this.fileList.poll()) != null) {
                    BasicFileAttributes attr = null;
                    try {
                        attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class, new LinkOption[0]);
                    }
                    catch (NoSuchFileException nExp) {
                        if (!this.logger.isDebugEnabled()) {
                            continue;
                        }
                        final String msg = "File {" + file.getName() + "} seems to be deleted externally, ignoring this file entry";
                        this.logger.debug((Object)msg);
                        continue;
                    }
                    final String fileKey = attr.fileKey().toString();
                    final File oldFile = this.keyToFileObjMap.get(fileKey);
                    final Long creationTime = attr.creationTime().toMillis();
                    if (oldFile == null) {
                        if (this.logger.isDebugEnabled()) {
                            this.logger.debug((Object)("New file {" + file.getName() + "} Key {" + fileKey + "} is created"));
                        }
                        this.onFileCreate(file);
                    }
                    else {
                        final BasicFileAttributes oldAttr = this.keytoFileAttrMap.get(fileKey);
                        if (oldAttr != null && topFileName == null && attr.size() > oldAttr.size()) {
                            if (this.logger.isDebugEnabled()) {
                                this.logger.debug((Object)("File {" + file.getName() + "} is modified"));
                            }
                            this.onFileModify(file);
                            this.keytoFileAttrMap.remove(fileKey);
                            this.ignoreKeyMap.remove(fileKey);
                            topFileName = file.getName();
                        }
                        if (!oldFile.getName().equals(file.getName())) {
                            boolean ignore = false;
                            if (!this.ignoreKeyMap.isEmpty() && this.ignoreKeyMap.get(fileKey) != null) {
                                ignore = true;
                            }
                            if (file.getName().equals(topFileName)) {
                                if (this.logger.isDebugEnabled()) {
                                    this.logger.debug((Object)("File {" + oldFile.getName() + "} is deleted and its key is reused"));
                                }
                                this.onFileDelete(fileKey, file.getName());
                                this.onFileCreate(file);
                                this.ignoreKeyMap.remove(fileKey);
                                if (!this.ignoreKeyMap.isEmpty() && this.logger.isDebugEnabled()) {
                                    this.logger.debug((Object)("Entries to Ignore {" + this.ignoreKeyMap.toString() + "}"));
                                }
                            }
                            else if (ignore) {
                                if (this.logger.isDebugEnabled()) {
                                    this.logger.debug((Object)("Ignoring {" + file.getName() + "} Key {" + fileKey + "} event"));
                                }
                            }
                            else {
                                if (this.logger.isDebugEnabled()) {
                                    this.logger.debug((Object)("File {" + oldFile.getName() + "} is renamed/moved as {" + file.getName() + "}"));
                                }
                                this.onFileCreate(file);
                                this.onFileDelete(fileKey, file.getName());
                            }
                        }
                    }
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)("Key {" + fileKey + "} File {" + file.getName() + "}"));
                    }
                    this.creationTimeMap.put(file.getName(), creationTime);
                    this.keyToFileObjMap.put(fileKey, file);
                    deletedFileMap.remove(fileKey);
                    this.keytoFileAttrMap.put(fileKey, attr);
                    ++fileCnt;
                }
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Fetched {" + fileCnt + "} files from {" + this.prop.directory + "}"));
                }
                if (!foundMatchedFile) {
                    this.keyToFileObjMap.clear();
                    this.keytoFileAttrMap.clear();
                    this.creationTimeMap.clear();
                }
                for (final Map.Entry<String, File> entry : deletedFileMap.entrySet()) {
                    final File obj = entry.getValue();
                    final String ignoreFk = this.ignoreKeyMap.get(entry.getKey());
                    if (ignoreFk != null) {
                        this.ignoreKeyMap.remove(ignoreFk);
                        if (this.logger.isDebugEnabled()) {
                            this.logger.debug((Object)("Assumed the file refers to {" + ignoreFk + "}, is deleted so from IgnoreKey list"));
                        }
                    }
                    else {
                        this.onFileDelete(entry.getKey(), obj.getName());
                    }
                    this.creationTimeMap.remove(obj.getName());
                    this.keyToFileObjMap.remove(entry.getKey());
                    this.keytoFileAttrMap.remove(entry.getKey());
                }
            }
            catch (IOException e) {
                this.logger.warn((Object)("Got exception while polling {" + this.prop.directory + "}, will try to poll again"), (Throwable)e);
            }
            try {
                Thread.sleep(sleepTime);
            }
            catch (InterruptedException ex) {}
        }
        if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)"Exiting NFSWatcher thread");
        }
    }
    
    static {
        NFSWatcher.DEFAULT_MAX_FILE_CNT = 10000;
    }
}
