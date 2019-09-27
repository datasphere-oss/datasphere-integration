package com.datasphere.source.lib.directory;

import org.apache.log4j.*;
import java.util.concurrent.*;
import java.io.*;
import com.datasphere.common.exc.*;
import com.datasphere.source.lib.utils.*;

import java.util.*;
import java.nio.file.attribute.*;
import java.nio.file.*;

public class RollingFileSequencer extends FileSequencer
{
    Logger logger;
    PriorityBlockingQueue<File> fileList;
    Map<String, File> keyToFileObjMap;
    Map<String, String> nameToKeyMap;
    Map<String, Integer> keyCreateCntMap;
    Map<String, Long> creationTimeMap;
    String lastAddedFile;
    long positionToStart;
    String lastKey;
    File lastFileObj;
    String ignoreDeleteFor;
    private static int DELETED_FILE_RETRY;
    int deletedFileRetry;
    
    public RollingFileSequencer(final Map<String, Object> map) throws IOException, AdapterException {
        super(map);
        this.logger = Logger.getLogger((Class)RollingFileSequencer.class);
        this.init();
    }
    
    private void init() {
        this.fileList = new PriorityBlockingQueue<File>(10, new DefaultFileComparator());
        this.keyToFileObjMap = new TreeMap<String, File>();
        this.nameToKeyMap = new TreeMap<String, String>();
        this.keyCreateCntMap = new TreeMap<String, Integer>();
        this.creationTimeMap = new TreeMap<String, Long>();
    }
    
    @Override
    public void onFileCreate(final File file) {
        this.addFile(file, true);
    }
    
    @Override
    public boolean isEmpty() {
        return this.fileList.size() == 0 && this.keyToFileObjMap.isEmpty();
    }
    
    @Override
    public void onFileDelete(final String fileKey, final String fileName) {
        Integer refCnt = this.keyCreateCntMap.get(fileKey);
        final File fileObj = this.keyToFileObjMap.get(fileKey);
        if (fileObj != null) {
            if (refCnt != null) {
                --refCnt;
                if (refCnt == 0) {
                    if (this.fileList.contains(fileObj)) {
                        if (this.logger.isDebugEnabled()) {
                            this.logger.debug((Object)("Zero reference filekey file {" + fileObj.getName() + "} Key {" + fileKey + "}, removing from file list. No of files to process {" + this.fileList.size() + "}"));
                        }
                        this.fileList.remove(fileObj);
                    }
                    else if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)("Zero reference filekey file {" + fileObj.getName() + "} Key {" + fileKey + "}, deleting filekey reference"));
                    }
                    this.keyToFileObjMap.remove(fileKey);
                    this.keyCreateCntMap.remove(fileKey);
                }
                else {
                    this.keyCreateCntMap.put(fileKey, refCnt);
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)("Reduced the reference count of {" + fileObj.getName() + "} Key {" + fileKey + "} to {" + refCnt + "}"));
                    }
                }
            }
            else {
                this.logger.debug((Object)"FileKey reference count can not be NULL, there is some logical issue");
            }
        }
        else {
            this.logger.debug((Object)"File is not seen before, ignoring the delete event");
        }
    }
    
    @Override
    public void onFileDelete(final File file) {
        synchronized (this.fileList) {
            final String fileName = file.getName();
            final String fileKey = this.nameToKeyMap.get(fileName);
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)("Got Delete event for {" + fileName + "} Key {" + fileKey + "}"));
            }
            if (fileKey == null) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("File {" + fileName + "} has no associated key in cache, ignoring delete event"));
                }
                return;
            }
            Integer refCnt = this.keyCreateCntMap.get(fileKey);
            final File fileObj = this.keyToFileObjMap.get(fileKey);
            if (fileObj != null) {
                if (refCnt != null) {
                    --refCnt;
                    if (refCnt == 0) {
                        if (this.fileList.contains(fileObj)) {
                            if (this.logger.isDebugEnabled()) {
                                this.logger.debug((Object)("Zero reference filekey file {" + file.getName() + "} Key {" + fileKey + "}, removing from file list. No of files to process {" + this.fileList.size() + "}"));
                            }
                            this.fileList.remove(fileObj);
                        }
                        else if (this.logger.isDebugEnabled()) {
                            this.logger.debug((Object)("Zero reference filekey file {" + file.getName() + "} Key {" + fileKey + "}, deleting filekey reference"));
                        }
                        this.keyToFileObjMap.remove(fileKey);
                        this.keyCreateCntMap.remove(fileKey);
                    }
                    else {
                        this.keyCreateCntMap.put(fileKey, refCnt);
                        if (this.logger.isDebugEnabled()) {
                            this.logger.debug((Object)("Reduced the reference count of {" + fileName + "} Key {" + fileKey + "} to {" + refCnt + "}"));
                        }
                    }
                }
                else {
                    this.logger.debug((Object)"FileKey reference count can not be NULL, there is some logical issue");
                }
            }
            else {
                this.logger.debug((Object)"File is not seen before, ignoring the delete event");
            }
        }
        if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)"End of onFileDelete()");
        }
    }
    
    @Override
    public void onFileModify(final File file) {
        this.addFile(file, false);
    }
    
    private void addFile(final File file, final boolean isCreateEvent) {
        synchronized (this.fileList) {
            try {
                final BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class, new LinkOption[0]);
                final String fileKey = attr.fileKey().toString();
                final String fileName = file.getName();
                final File oldFileObject = this.keyToFileObjMap.get(fileKey);
                final Long creationTime = attr.creationTime().toMillis();
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Got event for {" + fileKey + "} {" + fileName + "} Create {" + attr.creationTime().toMillis() + "} Lastmod {" + attr.lastModifiedTime().toMillis() + "}"));
                }
                final Integer cnt = this.keyCreateCntMap.get(fileKey);
                if (cnt == null) {
                    this.keyCreateCntMap.put(fileKey, 1);
                }
                else {
                    this.keyCreateCntMap.put(fileKey, cnt + 1);
                }
                this.ignoreDeleteFor = fileKey;
                if (oldFileObject == null) {
                    this.fileList.add(file);
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)("Seen new file entry {" + fileName + "} {" + fileKey + "} adding into processing list. No of Files to process {" + this.fileList.size() + "}"));
                    }
                }
                else if (this.lastKey != null && this.lastKey.equals(fileKey)) {
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)("Processed file is getting renamed from {" + oldFileObject.getName() + "} to {" + file.getName() + "}"));
                    }
                }
                else if (this.fileList.contains(oldFileObject)) {
                    this.fileList.remove(oldFileObject);
                    this.fileList.add(file);
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)("Removed {" + oldFileObject.getName() + "} and added {" + file.getName() + "} . No of files to process {" + this.fileList.size() + "}"));
                    }
                }
                else if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("File {" + fileName + "} Key {" + fileKey + "} is already processed"));
                }
                this.nameToKeyMap.put(fileName, fileKey);
                this.creationTimeMap.put(fileName, creationTime);
                this.keyToFileObjMap.put(fileKey, file);
            }
            catch (IOException e) {
                this.logger.warn((Object)"Got exception while getting file attribute", (Throwable)e);
            }
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)("Files to process {" + this.fileList.size() + "}"));
            }
        }
    }
    
    @Override
    public File getNextFile(final File file, final long position) {
        File newFile = null;
        BasicFileAttributes attr = null;
        synchronized (this.fileList) {
            this.positionToStart = 0L;
            if (file == null) {
                newFile = this.fileList.poll();
            }
            else if (this.lastKey == null) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.warn((Object)"LastKey is null, which is not suppose to be");
                }
                if (this.lastFileObj == null && this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)"Last file {NULL}");
                }
            }
            else {
                newFile = this.keyToFileObjMap.get(this.lastKey);
                if (newFile == null) {
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug((Object)("File key {" + this.lastKey + "} is not found in cache, moving to next file"));
                    }
                    newFile = this.fileList.poll();
                }
                else {
                    try {
                        attr = Files.readAttributes(newFile.toPath(), BasicFileAttributes.class, new LinkOption[0]);
                    }
                    catch (IOException e) {
                        if (this.deletedFileRetry++ <= RollingFileSequencer.DELETED_FILE_RETRY) {
                            if (this.logger.isDebugEnabled()) {
                                this.logger.warn((Object)("Got exception while fetching file attribute for {" + newFile.getName() + "} the file could have been renamed, let the caller to retry again"));
                            }
                            return null;
                        }
                        if (this.logger.isDebugEnabled()) {
                            this.logger.debug((Object)("Waited for {" + this.deletedFileRetry + "} iteration, assuming the file is permanently deleted from the system and proceeesding to next file"));
                        }
                        this.deletedFileRetry = 0;
                    }
                    if (attr != null) {
                        if (!this.lastKey.equals(attr.fileKey().toString())) {
                            if (this.logger.isDebugEnabled()) {
                                this.logger.debug((Object)("LastKey {" + this.lastKey + "} RefKey {" + attr.fileKey().toString() + "} Looks like the file is deleted or moved {" + file.getName() + "} key {" + this.lastKey + "}, let the caller to retry again"));
                            }
                            try {
                                Thread.sleep(10L);
                            }
                            catch (Exception ex) {}
                            return null;
                        }
                        if (attr.size() > position) {
                            if (this.logger.isDebugEnabled()) {
                                this.logger.debug((Object)("File {" + newFile.getName() + "} Key {" + this.lastKey + "} CurPos {" + position + "}:{" + (attr.size() - position) + "} more bytes to process"));
                            }
                            this.positionToStart = position;
                            return newFile;
                        }
                        newFile = this.fileList.poll();
                    }
                    else {
                        newFile = this.fileList.poll();
                    }
                }
            }
            if (newFile != null) {
                this.lastFileObj = newFile;
                this.lastKey = this.nameToKeyMap.get(newFile.getName());
                if (this.lastKey == null && this.logger.isDebugEnabled()) {
                    this.logger.warn((Object)("Null key found for {" + newFile.getName() + "}"));
                }
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Giving {" + newFile.getName() + "} StartPos {" + this.positionToStart + "} for processing, key {" + this.lastKey + "} {" + this.fileList.size() + "} more files to process"));
                }
            }
            else {
                this.lastFileObj = null;
            }
        }
        return newFile;
    }
    
    @Override
    public long getPosition() {
        return this.positionToStart;
    }
    
    @Override
    public boolean position(final String fileName) {
        synchronized (this.fileList) {
            File fileObj;
            while ((fileObj = this.fileList.peek()) != null) {
                if (fileObj != null && fileObj.getName().equals(fileName)) {
                    return true;
                }
                fileObj = this.fileList.poll();
            }
        }
        this.logger.warn((Object)("Couldn't find {" + fileName + "} for positioning"));
        return false;
    }
    
    @Override
    protected void postProcess(final File file) {
    }
    
    @Override
    protected void preProcess(final File file) {
    }
    
    @Override
    protected int fileListSize() {
        return 1;
    }
    
    static {
        RollingFileSequencer.DELETED_FILE_RETRY = 5;
    }
}
