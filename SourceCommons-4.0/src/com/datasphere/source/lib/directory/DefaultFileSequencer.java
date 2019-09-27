package com.datasphere.source.lib.directory;

import org.apache.log4j.*;
import java.util.concurrent.*;

import com.datasphere.source.lib.prop.*;
import com.datasphere.source.lib.utils.*;

import java.util.*;
import java.io.*;
import com.datasphere.common.exc.*;

public class DefaultFileSequencer extends FileSequencer
{
    Logger logger;
    PriorityBlockingQueue<Object> fileList;
    File lastFile;
    long recoverPosition;
    Map<String, File> nameToObjMap;
    String lastAddedFile;
    private boolean readFromSubDirectories;
    
    public DefaultFileSequencer(final Map<String, Object> map) throws IOException, AdapterException {
        super(map);
        this.logger = Logger.getLogger((Class)DefaultFileSequencer.class);
        this.lastAddedFile = "";
        this.readFromSubDirectories = false;
        this.fileList = new PriorityBlockingQueue<Object>(10, new DefaultFileComparator());
        this.nameToObjMap = new TreeMap<String, File>();
        final Property prop = new Property(map);
        this.readFromSubDirectories = prop.getBoolean("includesubdirectories", false);
    }
    
    @Override
    protected int fileListSize() {
        return this.fileList.size();
    }
    
    @Override
    public boolean isEmpty() {
        if (this.lastFile != null) {
            return !this.lastFile.exists() && this.fileList.size() == 0;
        }
        return this.fileList.size() == 0;
    }
    
    @Override
    public void onFileModify(final File file) {
        try {
            final String filename = this.getFilename(file);
            if (!this.lastAddedFile.equals(filename)) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Got file modification event for {" + filename + "}"));
                }
                this.lastAddedFile = filename;
                this.fileList.add(file);
            }
        }
        catch (IOException e) {
            this.logger.error((Object)("Unable to retrieve canonical pathname for the file " + file.getName()));
        }
    }
    
    @Override
    public void onFileDelete(final File file) {
        try {
            final String filename = this.getFilename(file);
            final File tmpFile = this.nameToObjMap.get(filename);
            if (tmpFile != null) {
                this.fileList.remove(tmpFile);
                this.nameToObjMap.remove(filename);
            }
        }
        catch (IOException ioe) {
            this.logger.error((Object)("Unable to retrieve canonical pathname for the file " + file.getName()));
        }
    }
    
    @Override
    public void onFileCreate(final File file) {
        try {
            final String filename = this.getFilename(file);
            if (!this.lastAddedFile.equals(filename) && this.nameToObjMap.get(filename) == null) {
                this.nameToObjMap.put(filename, file);
                this.fileList.add(file);
                this.lastAddedFile = filename;
            }
        }
        catch (IOException ioe) {
            this.logger.error((Object)("Unable to retrieve canonical pathname for the file " + file.getName()));
        }
    }
    
    @Override
    public boolean position(final String fileName) {
        if (this.nameToObjMap.get(fileName) != null) {
            File file;
            while ((file = (File)this.fileList.peek()) != null && this.fileList.size() > 0) {
                if (file != null) {
                    String fName = null;
                    try {
                        fName = this.getFilename(file);
                        this.nameToObjMap.remove(fName);
                        if (fName.equals(fileName)) {
                            return true;
                        }
                    }
                    catch (IOException e) {
                        this.logger.error((Object)("Unable to retrieve canonical pathname for the file " + file.getName()));
                    }
                }
                this.fileList.poll();
            }
        }
        return false;
    }
    
    @Override
    public File getNextFile(final File file, final long position) {
        final File nextFile = (File)this.fileList.poll();
        if (nextFile != null) {
            try {
                this.nameToObjMap.remove(this.getFilename(nextFile));
            }
            catch (IOException ioe) {
                this.logger.error((Object)("Unable to retrieve canonical pathname for the file " + file.getName()));
            }
        }
        return this.lastFile = nextFile;
    }
    
    @Override
    public void onFileDelete(final String fileKey, final String fileName) {
        this.logger.error((Object)"onFileDelete() with FileKey based implementation is not required here");
    }
    
    private String getFilename(final File file) throws IOException {
        if (this.readFromSubDirectories) {
            return file.getCanonicalPath();
        }
        return file.getName();
    }
}
