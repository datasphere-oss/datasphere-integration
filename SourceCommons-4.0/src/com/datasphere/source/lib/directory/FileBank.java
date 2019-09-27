package com.datasphere.source.lib.directory;

import org.apache.log4j.*;
import java.util.concurrent.atomic.*;
import java.util.*;
import com.datasphere.common.exc.*;
import com.datasphere.source.lib.prop.*;

import java.util.regex.*;
import java.io.*;
import java.nio.file.*;

public class FileBank extends Observable implements WatcherCallback
{
    static Logger logger;
    public static String INSTANCE;
    public static String OBSERVER;
    protected Map<String, FileSequencer> sequencerMap;
    protected static Map<String, Pattern> matcherMap;
    public static AtomicInteger instanceCnt;
    Watcher watcher;
    Observer observer;
    Subject subject;
    FileSequencer localSequencer;
    protected Property prop;
    
    public FileBank(final Property prop) throws AdapterException {
        this.prop = prop;
        this.sequencerMap = new TreeMap<String, FileSequencer>();
        this.observer = (Observer)prop.getMap().get(FileBank.OBSERVER);
        this.subject = new Subject();
        try {
            (this.watcher = WatcherFactory.createWatcher(prop.propMap, this)).init();
            this.localSequencer = this.createSequencer(prop);
            this.sequencerMap.put(prop.wildcard, this.localSequencer);
        }
        catch (IOException e) {
            throw new AdapterException("Got exception while creating directory instance", (Throwable)e);
        }
    }
    
    protected FileSequencer createSequencer(final Property prop) throws AdapterException {
        try {
            return FileSequencer.load(prop);
        }
        catch (AdapterException e) {
            throw new AdapterException("Got exception while creating Sequencer", (Throwable)e);
        }
    }
    
    public static String extractProcessGroup(final String pGroupRegEx, final File file) {
        Pattern pattern = null;
        if ((pattern = FileBank.matcherMap.get(pGroupRegEx)) == null) {
            pattern = Pattern.compile(pGroupRegEx);
            FileBank.matcherMap.put(pGroupRegEx, pattern);
        }
        final Matcher matcher = pattern.matcher(file.getName());
        if (matcher.find()) {
            return "*" + matcher.group() + "*";
        }
        return file.getName();
    }
    
    public void updateInstance(final Property prop) throws AdapterException {
    }
    
    public void start() throws AdapterException {
        if (FileBank.logger.isDebugEnabled()) {
            FileBank.logger.debug((Object)("Starting directory instance for {" + this.prop.wildcard + "}"));
        }
        this.watcher.start();
    }
    
    public void stop(final String wildcard) {
        if (FileBank.logger.isDebugEnabled()) {
            FileBank.logger.debug((Object)("Stopping directory instance for {" + wildcard + "}"));
        }
        this.sequencerMap.remove(wildcard);
        FileBank.instanceCnt.decrementAndGet();
        this.stop();
    }
    
    public void stop() {
        if (this.sequencerMap.isEmpty()) {
            if (FileBank.logger.isDebugEnabled()) {
                FileBank.logger.debug((Object)"No active groups, stopping watcher thead");
            }
            this.watcher.stop();
        }
        else if (FileBank.logger.isDebugEnabled()) {
            FileBank.logger.debug((Object)("No of active groups {" + this.sequencerMap.size() + "} found for {" + this.prop.wildcard + "}"));
        }
    }
    
    public boolean isEOFReached(final String wildcard, final InputStream in, final boolean fileChanged) throws IOException {
        return this.localSequencer.isEOFReached(in, fileChanged);
    }
    
    public boolean isEmpty(final String wildcard) {
        return this.localSequencer.isEmpty();
    }
    
    public FileDetails getNextFile(final String wildcard, final File file, final long position) {
        final File fileObj = this.localSequencer.getNextFile(file, position);
        if (fileObj != null) {
            final FileDetails fd = new FileDetails();
            fd.file = fileObj;
            fd.startPosition = this.localSequencer.getPosition();
            return fd;
        }
        return null;
    }
    
    public long getPosition(final String wildcard) {
        return this.localSequencer.getPosition();
    }
    
    public boolean position(final String wildcard, final String fileToRecover) {
        return this.localSequencer.position(fileToRecover);
    }
    
    @Override
    public void onFileCreate(final File file) {
        if (FileBank.logger.isDebugEnabled()) {
            FileBank.logger.debug((Object)("New file created {" + file.getName() + "}"));
        }
        this.localSequencer.onFileCreate(file);
    }
    
    @Override
    public void onFileDelete(final File file) {
        if (FileBank.logger.isDebugEnabled()) {
            FileBank.logger.debug((Object)("File {" + file.getName() + "} is deleted"));
        }
        this.localSequencer.onFileDelete(file);
    }
    
    @Override
    public void onFileDelete(final String fileKey, final String fileName) {
        if (FileBank.logger.isDebugEnabled()) {
            FileBank.logger.debug((Object)("FileKey {" + fileKey + "} {" + fileName + "} is deleted"));
        }
        this.localSequencer.onFileDelete(fileKey, fileName);
    }
    
    @Override
    public void onFileModify(final File file) {
        if (FileBank.logger.isDebugEnabled()) {
            FileBank.logger.debug((Object)("File {" + file.getName() + "} is modified"));
        }
        this.localSequencer.onFileModify(file);
    }
    
    static {
        FileBank.logger = Logger.getLogger((Class)FileBank.class);
        FileBank.INSTANCE = "DirectoryInstance";
        FileBank.OBSERVER = "Observer";
        FileBank.matcherMap = new TreeMap<String, Pattern>();
        FileBank.instanceCnt = new AtomicInteger(0);
    }
    
    public class Subject
    {
        public File file;
        public WatchEvent.Kind<Path> event;
    }
    
    public class FileDetails
    {
        public File file;
        public long startPosition;
    }
}
